use super::*;
use crate::db::manifest::{GraphHeadExpectation, LineageIntent};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Exact source bytes and accepted identity-bearing schema, rather than shape alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaContractDigest {
    pub source_hash: String,
    pub schema_ir_hash: String,
    pub schema_identity_domain: String,
    pub schema_identity_version: u32,
}

impl SchemaContractDigest {
    pub(super) fn from_row(row: &SchemaContractRow) -> Self {
        Self {
            source_hash: source_hash(&row.source),
            schema_ir_hash: row.head.schema_ir_hash.clone(),
            schema_identity_domain: row.head.schema_identity_domain.clone(),
            schema_identity_version: row.head.schema_identity_version,
        }
    }
}

fn source_hash(source: &str) -> String {
    format!("{:x}", Sha256::digest(source.as_bytes()))
}

/// Engine-issued schema intent. Persist it before invoking effects if recovery
/// is required. Serialization is an internal v0.12 protocol, not an authorization
/// capability: execution always rechecks the supplied actor and current policy.
/// The caller owns durable storage, input-size bounds, retention and fencing.
/// Version 2 can publish only at its numeric base plus one; version 1 refuses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedSchemaApply {
    version: u32,
    root: String,
    pub(super) authority: GraphHeadExpectation,
    pub(super) base_manifest_version: u64,
    pub(super) base_contract: SchemaContractDigest,
    pub(super) desired_source: String,
    pub(super) desired_contract: SchemaContractDigest,
    actor: Option<String>,
    pub(super) lineage: Option<LineageIntent>,
}

impl PreparedSchemaApply {
    pub fn graph_commit_id(&self) -> Option<&str> {
        self.lineage
            .as_ref()
            .map(|intent| intent.graph_commit_id.as_str())
    }

    pub fn is_noop(&self) -> bool {
        self.lineage.is_none()
    }

    pub fn base_manifest_version(&self) -> u64 {
        self.base_manifest_version
    }

    pub fn base_head_commit_id(&self) -> Option<&str> {
        self.authority.head_commit_id.as_deref()
    }

    pub fn actor(&self) -> Option<&str> {
        self.actor.as_deref()
    }

    /// The exact accepted contract against which this intent was prepared.
    pub fn base_contract(&self) -> &SchemaContractDigest {
        &self.base_contract
    }

    pub fn desired_contract(&self) -> &SchemaContractDigest {
        &self.desired_contract
    }

    pub(super) fn validate_envelope(&self, db: &Omnigraph, actor: Option<&str>) -> Result<()> {
        self.validate_structure(db)?;
        if self.actor.as_deref() != actor {
            return Err(OmniError::manifest_conflict(
                "schema publication intent actor does not match its executor",
            ));
        }
        Ok(())
    }

    pub(super) fn validate_structure(&self, db: &Omnigraph) -> Result<()> {
        if self.version != 2
            || self.root != write_queue_root_identity(&db.root_uri)?
            || self.authority.branch.is_some()
            || self.authority.branch_identifier != lance::dataset::refs::BranchIdentifier::main()
            || self.base_manifest_version == 0
            || self.base_manifest_version == u64::MAX
            || self.desired_contract.source_hash != source_hash(&self.desired_source)
            || self.desired_contract.schema_identity_domain
                != self.base_contract.schema_identity_domain
            || self.desired_contract.schema_identity_version
                != self.base_contract.schema_identity_version
            || self.is_noop() != (self.base_contract == self.desired_contract)
        {
            return Err(OmniError::manifest_conflict(
                "invalid schema publication intent or actor/root binding",
            ));
        }
        if let Some(lineage) = &self.lineage
            && (lineage.branch.is_some()
                || lineage.merged_parent.is_some()
                || lineage.actor_id != self.actor
                || !lineage
                    .graph_commit_id
                    .parse::<ulid::Ulid>()
                    .is_ok_and(|id| id.to_string() == lineage.graph_commit_id)
                || self.authority.head_commit_id.as_ref() == Some(&lineage.graph_commit_id))
        {
            return Err(OmniError::manifest_conflict(
                "invalid schema publication lineage intent",
            ));
        }
        Ok(())
    }

    pub(super) fn validate_capture(
        &self,
        db: &Omnigraph,
        actor: Option<&str>,
        captured: &CapturedSchemaApply,
    ) -> Result<()> {
        self.validate_envelope(db, actor)?;
        if !self.matches_base(captured) || self.desired_contract != captured.desired_contract {
            return Err(OmniError::manifest_read_set_changed(
                "prepared_schema_authority",
                self.authority.head_commit_id.clone(),
                captured.graph_head.clone(),
            ));
        }
        Ok(())
    }

    fn matches_base(&self, captured: &CapturedSchemaApply) -> bool {
        self.base_manifest_version == captured.snapshot.graph_manifest_version()
            && self.authority.branch_identifier == captured.branch_identifier
            && self.authority.head_commit_id == captured.graph_head
            && self.base_contract == captured.base_contract
    }
}

/// Positive evidence only. `Unknown` does not mean absent, failed, or safe to
/// replay. This API neither publishes nor protects evidence from cleanup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchemaApplyReconciliation {
    Committed {
        commit: GraphCommit,
        contract: SchemaContractDigest,
    },
    NoOp {
        graph_manifest_version: u64,
        head_commit_id: Option<String>,
        contract: SchemaContractDigest,
    },
    Unknown,
}

pub(super) struct CapturedSchemaApply {
    pub(super) planned: PlannedSchemaApply,
    pub(super) accepted_catalog: Arc<Catalog>,
    pub(super) accepted_identity: SchemaContractIdentity,
    pub(super) snapshot: Snapshot,
    pub(super) branch_identifier: lance::dataset::refs::BranchIdentifier,
    pub(super) graph_head: Option<String>,
    base_contract: SchemaContractDigest,
    desired_contract: SchemaContractDigest,
}

impl CapturedSchemaApply {
    pub(super) fn issue(
        &self,
        db: &Omnigraph,
        source: &str,
        actor: Option<&str>,
    ) -> Result<PreparedSchemaApply> {
        let lineage = if self.base_contract == self.desired_contract {
            None
        } else {
            Some(GraphCoordinator::new_lineage_intent_for_branch(
                None,
                actor,
                None,
                HistoryReleaseBytes::PRODUCTION,
            )?)
        };
        Ok(PreparedSchemaApply {
            version: 2,
            root: write_queue_root_identity(&db.root_uri)?,
            authority: GraphHeadExpectation::new(
                None,
                self.branch_identifier.clone(),
                self.graph_head.clone(),
            ),
            base_manifest_version: self.snapshot.graph_manifest_version(),
            base_contract: self.base_contract.clone(),
            desired_source: source.to_string(),
            desired_contract: self.desired_contract.clone(),
            actor: actor.map(str::to_string),
            lineage,
        })
    }
}

pub(super) fn authorize(db: &Omnigraph, actor: Option<&str>) -> Result<()> {
    db.enforce(
        omnigraph_policy::PolicyAction::SchemaApply,
        &omnigraph_policy::ResourceScope::TargetBranch("main".to_string()),
        actor,
    )
}

pub(super) async fn capture(
    db: &Omnigraph,
    source: &str,
    prepared: Option<&PreparedSchemaApply>,
) -> Result<CapturedSchemaApply> {
    db.refresh_coordinator_only().await?;
    let (snapshot, branch_identifier, graph_head) = {
        let coordinator = db.coordinator.read().await;
        (
            coordinator.snapshot(),
            coordinator.branch_identifier().await?,
            coordinator.exact_graph_head(),
        )
    };
    if snapshot.graph_branch().is_some() {
        return Err(OmniError::manifest_conflict(
            "schema publication requires main",
        ));
    }
    let (accepted_catalog, accepted_identity) = db.accepted_catalog_for_snapshot(&snapshot).await?;
    let accepted_ir = accepted_catalog.bound_schema_ir().ok_or_else(|| {
        OmniError::manifest_internal("accepted catalog carries no bound SchemaIR")
    })?;
    let base_contract =
        SchemaContractDigest::from_row(&snapshot.read_schema_contract(&db.root_uri).await?);
    // Refuse stale input before resolving desired identities against a newer
    // schema. Even a now-unsupported migration is a stale prepared attempt.
    if let Some(prepared) = prepared
        && (prepared.base_manifest_version != snapshot.graph_manifest_version()
            || prepared.authority.branch_identifier != branch_identifier
            || prepared.authority.head_commit_id != graph_head
            || prepared.base_contract != base_contract)
    {
        return Err(OmniError::manifest_read_set_changed(
            "prepared_schema_authority",
            prepared.authority.head_commit_id.clone(),
            graph_head,
        ));
    }
    let planned = plan_schema_for_apply_from_accepted(db, source, accepted_ir).await?;
    let desired_contract =
        SchemaContractDigest::from_row(&render_schema_contract(&planned.desired_ir, source)?);
    Ok(CapturedSchemaApply {
        planned,
        accepted_catalog,
        accepted_identity,
        snapshot,
        branch_identifier,
        graph_head,
        base_contract,
        desired_contract,
    })
}

pub(in crate::db::omnigraph) async fn prepare_schema_apply(
    db: &Omnigraph,
    source: &str,
    actor: Option<&str>,
) -> Result<PreparedSchemaApply> {
    authorize(db, actor)?;
    let _schema_gate = db.write_queue().acquire_schema_exclusive().await;
    let captured = capture(db, source, None)
        .await
        .map_err(OmniError::before_effect)?;
    captured.issue(db, source, actor)
}

pub(in crate::db::omnigraph) async fn reconcile_schema_apply(
    db: &Omnigraph,
    prepared: &PreparedSchemaApply,
    actor: Option<&str>,
) -> Result<SchemaApplyReconciliation> {
    authorize(db, actor)?;
    prepared.validate_envelope(db, actor)?;
    if prepared.is_noop() {
        let _schema_gate = db.write_queue().acquire_schema_exclusive().await;
        db.refresh_coordinator_only().await?;
        let (snapshot, branch_identifier, graph_head) = {
            let coordinator = db.coordinator.read().await;
            (
                coordinator.snapshot(),
                coordinator.branch_identifier().await?,
                coordinator.exact_graph_head(),
            )
        };
        let row = snapshot.read_schema_contract(&db.root_uri).await?;
        validate_schema_contract_row(&row)?;
        let only_main = db
            .coordinator
            .read()
            .await
            .all_branches()
            .await?
            .iter()
            .all(|branch| branch == "main");
        return Ok(
            if only_main
                && snapshot.graph_branch().is_none()
                && snapshot.graph_manifest_version() == prepared.base_manifest_version
                && branch_identifier == prepared.authority.branch_identifier
                && graph_head == prepared.authority.head_commit_id
                && SchemaContractDigest::from_row(&row) == prepared.desired_contract
            {
                SchemaApplyReconciliation::NoOp {
                    graph_manifest_version: prepared.base_manifest_version,
                    head_commit_id: prepared.authority.head_commit_id.clone(),
                    contract: prepared.desired_contract.clone(),
                }
            } else {
                SchemaApplyReconciliation::Unknown
            },
        );
    }
    // Exact candidate only: no branch fanout, history search, or mutable HEAD
    // as a receipt. Head-preserving metadata advances may make this unknown.
    let Some(version) = prepared.base_manifest_version.checked_add(1) else {
        return Ok(SchemaApplyReconciliation::Unknown);
    };
    let intent = prepared
        .lineage
        .as_ref()
        .expect("effectful intent has lineage");
    let Some(evidence) = omnigraph_catalog::read_schema_publication_at(
        &db.root_uri,
        version,
        &intent.graph_commit_id,
    )
    .await?
    else {
        return Ok(SchemaApplyReconciliation::Unknown);
    };
    validate_schema_contract_row(&evidence.contract)?;
    let contract = SchemaContractDigest::from_row(&evidence.contract);
    let commit = evidence.commit;
    if evidence.branch_identifier != prepared.authority.branch_identifier
        || contract != prepared.desired_contract
        || commit.parent_commit_id != prepared.authority.head_commit_id
        || commit.actor_id != intent.actor_id
        || commit.created_at != intent.created_at
        || commit.merged_parent_commit_id.is_some()
    {
        return Err(OmniError::manifest_conflict(
            "schema publication evidence does not match its intent",
        ));
    }
    Ok(SchemaApplyReconciliation::Committed { commit, contract })
}
