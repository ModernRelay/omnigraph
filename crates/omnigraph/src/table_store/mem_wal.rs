//! Narrow RFC-026 adapters for bounded MemWAL enrollment and the private
//! B1/common-B2 core.
//!
//! The parent module captures exact main-branch witnesses, initializes the
//! singleton unsharded MemWAL index, provisions pre-minted empty shards, and
//! classifies enrollment or sealed-rebind effects after a lost result. Rebind
//! replaces only index authority, retains every authenticated old shard prefix,
//! and obtains one fresh fence-only claim without a put. The private [`worker`]
//! submodule owns B1's one-generation admission, watcher,
//! post-durability successor-epoch check, replay, seal/drain, and quiesced
//! retirement mechanics. Current schema v9 adds common-B2 token/attribution;
//! graph authority, recovery-v12 base-plus-token fold ownership, and the sole
//! `__manifest` visibility publication remain in
//! `db::omnigraph::stream_ingest`; no production streaming API is exposed.
//!
//! The caller must hold RFC-026's exclusive base-HEAD and cleanup/GC gates from
//! the final pre-effect check through classification and manifest publication.
//! Those gates are what make an absent immediate successor proof meaningful.

mod worker;

pub(crate) use worker::*;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;

use futures::{TryStream, TryStreamExt};
use lance::Dataset;
use lance::dataset::mem_wal::{DatasetMemWalExt, ShardManifestStore, ShardWriterConfig, WalTailer};
use lance::dataset::transaction::Operation;
use lance::index::DatasetIndexExt;
use lance_index::mem_wal::{
    MEM_WAL_INDEX_NAME, MemWalIndexDetails, ShardId, ShardManifest, ShardStatus,
};
use object_store::{ObjectMeta, path::Path};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::db::manifest::{CurrentHeadWitness, STREAM_CONFIG_VERSION, StreamPhysicalBinding};

const UNSHARDED_SPEC_ID: u32 = 1;
const SHARD_MANIFEST_SCAN_BATCH_SIZE: usize = 2;
/// A valid fresh shard has two objects (manifest v1 plus its optional hint).
/// Eight leaves enough room to describe a small corrupt/foreign inventory while
/// placing a hard ceiling on evidence collection.  More state is ambiguous, not
/// permission to keep scanning an unbounded prefix.
const MAX_MEM_WAL_INVENTORY_OBJECTS: usize = 8;
/// Binding ancestry is fixed-size ledger state. Rebind may retain every prior
/// shard prefix, but it must never turn a root listing into an unbounded scan.
const MAX_RETAINED_REBIND_SHARDS: usize = 1_024;
/// Only the fresh shard is inspected recursively. This bound permits several
/// manifest-only claim attempts plus Lance's manifest hint and the one
/// required fence sentinel, while still making provider residue loud.
const MAX_REBIND_FRESH_SHARD_OBJECTS: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum MemWalEnrollmentError {
    #[error("invalid MemWAL enrollment plan: {reason}")]
    InvalidPlan { reason: String },
    #[error("MemWAL enrollment supports only the main table branch, got {branch:?}")]
    UnsupportedBranch { branch: Option<String> },
    #[error("table is not eligible for MemWAL enrollment: {reason}")]
    UnsupportedTable { reason: String },
    #[error("captured MemWAL enrollment authority changed: {reason}")]
    AuthorityChanged { reason: String },
    #[error("MemWAL enrollment physical state conflicts with the intent: {reason}")]
    PhysicalConflict { reason: String },
    #[error("MemWAL inventory exceeded {limit} objects under {scope}")]
    InventoryLimitExceeded { scope: String, limit: usize },
    #[error("Lance MemWAL {operation} failed: {message}")]
    Lance {
        operation: &'static str,
        message: String,
    },
}

impl MemWalEnrollmentError {
    fn lance(operation: &'static str, error: impl Display) -> Self {
        Self::Lance {
            operation,
            message: error.to_string(),
        }
    }

    fn authority(reason: impl Into<String>) -> Self {
        Self::AuthorityChanged {
            reason: reason.into(),
        }
    }

    fn physical(reason: impl Into<String>) -> Self {
        Self::PhysicalConflict {
            reason: reason.into(),
        }
    }
}

/// Fixed bounded-profile enrollment intent.
///
/// Both identities are pre-minted before the recovery intent is armed.  The
/// enrollment identity is persisted independently of Lance's replaceable
/// MemWAL index UUID; the shard identity names the only admitted shard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MemWalEnrollmentPlan {
    pub(crate) enrollment_id: ShardId,
    pub(crate) shard_id: ShardId,
}

impl MemWalEnrollmentPlan {
    pub(crate) fn new(
        enrollment_id: ShardId,
        shard_id: ShardId,
    ) -> Result<Self, MemWalEnrollmentError> {
        let plan = Self {
            enrollment_id,
            shard_id,
        };
        plan.validate()?;
        Ok(plan)
    }

    /// Re-run constructor invariants after deserializing a recovery sidecar.
    pub(crate) fn validate(&self) -> Result<(), MemWalEnrollmentError> {
        if self.enrollment_id.is_nil() {
            return Err(MemWalEnrollmentError::InvalidPlan {
                reason: "enrollment_id must be non-nil".to_string(),
            });
        }
        if self.shard_id.is_nil() {
            return Err(MemWalEnrollmentError::InvalidPlan {
                reason: "shard_id must be non-nil".to_string(),
            });
        }
        if self.enrollment_id.get_version_num() != 4 || self.shard_id.get_version_num() != 4 {
            return Err(MemWalEnrollmentError::InvalidPlan {
                reason: "enrollment_id and shard_id must be UUID v4 values".to_string(),
            });
        }
        if self.enrollment_id == self.shard_id {
            return Err(MemWalEnrollmentError::InvalidPlan {
                reason: "enrollment_id and shard_id must be distinct identities".to_string(),
            });
        }
        Ok(())
    }

    /// Stable digest of every fixed physical configuration choice in the
    /// bounded profile.  Enrollment and shard identities are deliberately not
    /// included: they are carried separately in the physical binding.
    pub(crate) fn stream_config_hash(&self) -> String {
        stream_config_v3_hash()
    }

    fn expected_writer_defaults(&self) -> BTreeMap<String, String> {
        expected_b1_writer_defaults(self.enrollment_id)
    }

    /// RC.1 persists defaults but does not apply them to caller-created writer
    /// configs.  Re-read and exactly validate the durable configuration, then
    /// reconstruct the writer config from those persisted values.
    fn reconstruct_writer_config(
        &self,
        details: &MemWalIndexDetails,
    ) -> Result<ShardWriterConfig, MemWalEnrollmentError> {
        validate_fresh_unsharded_details(details, self)?;
        reconstruct_b1_writer_config(details, self.enrollment_id, self.shard_id)
            .map_err(|error| MemWalEnrollmentError::physical(error.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MemWalEnrollmentReceipt {
    pub(crate) head: CurrentHeadWitness,
    /// Observable effect identity only.  It is not stable enrollment identity.
    pub(crate) mem_wal_index_uuid: ShardId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MemWalEnrollmentState {
    ExactNoEffect,
    ExactIndexOnly(MemWalEnrollmentReceipt),
    ExactIndexAndExpectedEmptyShard {
        receipt: MemWalEnrollmentReceipt,
        shard_manifest: ShardManifest,
    },
}

/// Exact physical intent for replacing one sealed lane's MemWAL binding.
///
/// `retained_shard_ids` is not ambient object-store discovery: the caller must
/// derive the canonical set from the authenticated binding-receipt ancestry
/// selected by durable token authority. The adapter compares that complete set
/// with the root prefixes and never adopts an unmentioned shard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MemWalRebindPlan {
    pub(crate) prior_binding: StreamPhysicalBinding,
    pub(crate) retained_shard_ids: Vec<ShardId>,
    pub(crate) next_enrollment: MemWalEnrollmentPlan,
}

impl MemWalRebindPlan {
    pub(crate) fn new(
        prior_binding: StreamPhysicalBinding,
        retained_shard_ids: Vec<ShardId>,
        next_enrollment: MemWalEnrollmentPlan,
    ) -> Result<Self, MemWalEnrollmentError> {
        let plan = Self {
            prior_binding,
            retained_shard_ids,
            next_enrollment,
        };
        plan.validate()?;
        Ok(plan)
    }

    /// Re-run constructor invariants after deserializing recovery authority.
    pub(crate) fn validate(&self) -> Result<(), MemWalEnrollmentError> {
        self.next_enrollment.validate()?;
        if self.prior_binding.table_branch.is_some()
            || self.prior_binding.shard_ids.len() != 1
            || self.prior_binding.stream_config_version != STREAM_CONFIG_VERSION
            || self.prior_binding.stream_config_hash != stream_config_v3_hash()
        {
            return Err(MemWalEnrollmentError::InvalidPlan {
                reason: "prior binding is not the exact main-only config-v3 singleton".to_string(),
            });
        }
        self.prior_binding
            .identity()
            .map_err(|error| MemWalEnrollmentError::InvalidPlan {
                reason: format!("prior binding identity is invalid: {error}"),
            })?;
        let prior_enrollment =
            ShardId::parse_str(&self.prior_binding.enrollment_id).map_err(|error| {
                MemWalEnrollmentError::InvalidPlan {
                    reason: format!("prior enrollment_id is invalid: {error}"),
                }
            })?;
        let prior_shard =
            ShardId::parse_str(&self.prior_binding.shard_ids[0]).map_err(|error| {
                MemWalEnrollmentError::InvalidPlan {
                    reason: format!("prior shard_id is invalid: {error}"),
                }
            })?;
        if prior_enrollment.is_nil()
            || prior_shard.is_nil()
            || prior_enrollment.get_version_num() != 4
            || prior_shard.get_version_num() != 4
            || prior_enrollment == prior_shard
        {
            return Err(MemWalEnrollmentError::InvalidPlan {
                reason: "prior enrollment_id and shard_id must be distinct UUID v4 values"
                    .to_string(),
            });
        }
        if self.retained_shard_ids.is_empty()
            || self.retained_shard_ids.len() > MAX_RETAINED_REBIND_SHARDS
        {
            return Err(MemWalEnrollmentError::InvalidPlan {
                reason: format!(
                    "retained shard ancestry must contain 1..={MAX_RETAINED_REBIND_SHARDS} identities"
                ),
            });
        }
        let mut canonical = self.retained_shard_ids.clone();
        canonical.sort_unstable();
        canonical.dedup();
        if canonical != self.retained_shard_ids {
            return Err(MemWalEnrollmentError::InvalidPlan {
                reason: "retained shard ancestry must be sorted and unique".to_string(),
            });
        }
        if canonical
            .iter()
            .any(|shard| shard.is_nil() || shard.get_version_num() != 4)
        {
            return Err(MemWalEnrollmentError::InvalidPlan {
                reason: "retained shard ancestry must contain only UUID v4 identities".to_string(),
            });
        }
        if !canonical.contains(&prior_shard) {
            return Err(MemWalEnrollmentError::InvalidPlan {
                reason: "retained shard ancestry omits the selected prior shard".to_string(),
            });
        }
        if canonical.contains(&self.next_enrollment.shard_id)
            || canonical.contains(&self.next_enrollment.enrollment_id)
            || prior_enrollment == self.next_enrollment.enrollment_id
            || prior_shard == self.next_enrollment.enrollment_id
            || prior_enrollment == self.next_enrollment.shard_id
        {
            return Err(MemWalEnrollmentError::InvalidPlan {
                reason: "fresh enrollment and shard identities overlap retained authority"
                    .to_string(),
            });
        }
        Ok(())
    }

    fn prior_shard_id(&self) -> ShardId {
        ShardId::parse_str(&self.prior_binding.shard_ids[0]).expect("validated rebind prior shard")
    }

    pub(crate) fn expected_terminal_shards(&self) -> Vec<ShardId> {
        let mut expected = self.retained_shard_ids.clone();
        expected.push(self.next_enrollment.shard_id);
        expected.sort_unstable();
        expected
    }
}

/// Exact fence-only facts produced by the fresh binding's initial claim.
/// Protocol digests stay in lifecycle/recovery code because they additionally
/// bind graph identity, stream incarnation, and the new binding scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MemWalRebindFenceEvidence {
    pub(crate) shard_manifest: ShardManifest,
    pub(crate) sentinel_position: u64,
    pub(crate) sentinel_writer_epoch: u64,
    pub(crate) base_merged_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MemWalRebindState {
    ExactNoEffect,
    ExactIndexDropped {
        dropped_head: CurrentHeadWitness,
    },
    ExactFreshIndexOnly {
        dropped_head: CurrentHeadWitness,
        receipt: MemWalEnrollmentReceipt,
    },
    /// Epoch 1 provisions the fresh namespace but has no predecessor and thus
    /// no fence sentinel. Rebind cannot publish from this intermediate state.
    ExactFreshIndexAndProvisionedShard {
        dropped_head: CurrentHeadWitness,
        receipt: MemWalEnrollmentReceipt,
        shard_manifest: ShardManifest,
    },
    /// A claim advanced the manifest but did not durably expose its sentinel.
    /// A retry may claim the next epoch; no data or lifecycle authority exists.
    ExactFreshIndexAndClaimManifestOnly {
        dropped_head: CurrentHeadWitness,
        receipt: MemWalEnrollmentReceipt,
        shard_manifest: ShardManifest,
    },
    ExactFreshIndexAndFenceOnlyClaim {
        dropped_head: CurrentHeadWitness,
        receipt: MemWalEnrollmentReceipt,
        evidence: MemWalRebindFenceEvidence,
    },
}

#[derive(Debug, Default, PartialEq, Eq)]
struct RawMemWalInventory {
    objects_by_shard: BTreeMap<ShardId, Vec<String>>,
    malformed_prefixes: BTreeSet<String>,
    loose_objects: Vec<String>,
}

impl RawMemWalInventory {
    fn is_empty(&self) -> bool {
        self.objects_by_shard.is_empty()
            && self.malformed_prefixes.is_empty()
            && self.loose_objects.is_empty()
    }
}

/// Capture the exact no-MemWAL baseline selected by the caller's durable
/// manifest authority.  This function does not resolve latest.
#[allow(dead_code)] // Reached by the intentionally dormant Phase-A orchestrator.
pub(crate) async fn capture_pre_enrollment_head(
    dataset: &Dataset,
) -> Result<CurrentHeadWitness, MemWalEnrollmentError> {
    require_bounded_profile_table(dataset)?;
    if dataset
        .mem_wal_index_details()
        .await
        .map_err(|error| MemWalEnrollmentError::lance("baseline index read", error))?
        .is_some()
    {
        return Err(MemWalEnrollmentError::physical(
            "baseline already contains the singleton MemWAL index",
        ));
    }
    let inventory = raw_mem_wal_inventory(dataset).await?;
    if !inventory.is_empty() {
        return Err(MemWalEnrollmentError::physical(format!(
            "no-index baseline already contains MemWAL artifacts: {inventory:?}"
        )));
    }
    capture_current_head_witness(dataset).await
}

/// Capture a witness from a Dataset already opened at the exact version named
/// by durable graph authority.  The exclusive table gate must remain held
/// while the witness is consumed; rereading a pinned Dataset cannot detect a
/// concurrent latest-HEAD advance by itself.
pub(crate) async fn capture_current_head_witness(
    dataset: &Dataset,
) -> Result<CurrentHeadWitness, MemWalEnrollmentError> {
    require_main(dataset)?;
    let branch_before = dataset
        .branch_identifier()
        .await
        .map_err(|error| MemWalEnrollmentError::lance("branch witness read", error))?;
    let table_version = dataset.version().version;
    let transaction = dataset
        .read_transaction()
        .await
        .map_err(|error| MemWalEnrollmentError::lance("transaction witness read", error))?
        .ok_or_else(|| MemWalEnrollmentError::authority("current HEAD has no transaction"))?;
    if transaction.uuid.is_empty() {
        return Err(MemWalEnrollmentError::authority(
            "current HEAD transaction UUID is empty",
        ));
    }
    let manifest_e_tag = dataset.manifest_location().e_tag.clone();
    let branch_after = dataset
        .branch_identifier()
        .await
        .map_err(|error| MemWalEnrollmentError::lance("branch witness recheck", error))?;
    if branch_before != branch_after || dataset.version().version != table_version {
        return Err(MemWalEnrollmentError::authority(
            "exact-version witness is internally incoherent",
        ));
    }
    Ok(CurrentHeadWitness {
        branch_identifier: branch_before,
        table_version,
        transaction_uuid: transaction.uuid,
        manifest_e_tag,
    })
}

/// Read-only exact classifier for the bounded two-effect enrollment protocol.
///
/// Table history is never listed or resolved through latest.  The only
/// admitted attached chain is `N`, optional `N+1`, and absent `N+2`.
pub(crate) async fn classify_enrollment(
    anchor: &Dataset,
    before: &CurrentHeadWitness,
    plan: &MemWalEnrollmentPlan,
) -> Result<MemWalEnrollmentState, MemWalEnrollmentError> {
    plan.validate()?;
    require_bounded_profile_table(anchor)?;
    let inventory = raw_mem_wal_inventory(anchor).await?;
    let successor = exact_enrollment_successor_dataset(anchor, before).await?;

    let Some(successor) = successor else {
        if !inventory.is_empty() {
            return Err(MemWalEnrollmentError::physical(format!(
                "captured exact no-effect state contains MemWAL artifacts: {inventory:?}"
            )));
        }
        return Ok(MemWalEnrollmentState::ExactNoEffect);
    };

    let receipt = classify_exact_index_successor(&successor, before, plan).await?;
    if inventory.is_empty() {
        return Ok(MemWalEnrollmentState::ExactIndexOnly(receipt));
    }

    if !inventory.malformed_prefixes.is_empty() || !inventory.loose_objects.is_empty() {
        return Err(MemWalEnrollmentError::physical(format!(
            "MemWAL root contains malformed prefixes or loose objects: {inventory:?}"
        )));
    }
    if inventory.objects_by_shard.len() != 1
        || !inventory.objects_by_shard.contains_key(&plan.shard_id)
    {
        return Err(MemWalEnrollmentError::physical(format!(
            "MemWAL root is not exactly the pre-minted singleton shard {}: {inventory:?}",
            plan.shard_id
        )));
    }
    let shard_objects = inventory
        .objects_by_shard
        .get(&plan.shard_id)
        .expect("checked singleton shard above");
    let shard_manifest =
        exact_empty_shard_manifest(&successor, plan.shard_id, shard_objects).await?;
    Ok(MemWalEnrollmentState::ExactIndexAndExpectedEmptyShard {
        receipt,
        shard_manifest,
    })
}

/// Execute only the index-initialization effect, and only from a freshly
/// reclassified exact no-effect state.  The caller arms durable recovery first.
#[allow(dead_code)] // Reached by the intentionally dormant Phase-A orchestrator.
pub(crate) async fn initialize_index_from_exact_no_effect(
    dataset: &mut Dataset,
    before: &CurrentHeadWitness,
    plan: &MemWalEnrollmentPlan,
) -> Result<MemWalEnrollmentReceipt, MemWalEnrollmentError> {
    match classify_enrollment(dataset, before, plan).await? {
        MemWalEnrollmentState::ExactNoEffect => {}
        state => {
            return Err(MemWalEnrollmentError::physical(format!(
                "index initialization requires exact no-effect state, got {state:?}"
            )));
        }
    }

    let mut builder = dataset.initialize_mem_wal().unsharded();
    for (key, value) in plan.expected_writer_defaults() {
        builder = builder.add_writer_config_default(key, value);
    }
    builder
        .execute()
        .await
        .map_err(|error| MemWalEnrollmentError::lance("index initialization", error))?;

    match classify_enrollment(dataset, before, plan).await? {
        MemWalEnrollmentState::ExactIndexOnly(receipt) => Ok(receipt),
        state => Err(MemWalEnrollmentError::physical(format!(
            "initializer did not produce the exact index-only state: {state:?}"
        ))),
    }
}

/// Provision and close the pre-minted shard only from the exact index-only
/// state.  Reclassification before opening prevents an existing shard from
/// being claimed again and advancing its epoch.  No data is ever passed to the
/// writer.
pub(crate) async fn provision_shard_from_exact_index_only(
    anchor: &Dataset,
    before: &CurrentHeadWitness,
    plan: &MemWalEnrollmentPlan,
) -> Result<(MemWalEnrollmentReceipt, ShardManifest), MemWalEnrollmentError> {
    let receipt = match classify_enrollment(anchor, before, plan).await? {
        MemWalEnrollmentState::ExactIndexOnly(receipt) => receipt,
        state => {
            return Err(MemWalEnrollmentError::physical(format!(
                "shard provisioning requires exact index-only state, got {state:?}"
            )));
        }
    };
    let successor = anchor
        .checkout_version(receipt.head.table_version)
        .await
        .map_err(|error| MemWalEnrollmentError::lance("exact successor reopen", error))?;
    let details = successor
        .mem_wal_index_details()
        .await
        .map_err(|error| MemWalEnrollmentError::lance("writer defaults read", error))?
        .ok_or_else(|| MemWalEnrollmentError::physical("exact successor lost MemWAL details"))?;
    let config = plan.reconstruct_writer_config(&details)?;
    let writer = successor
        .mem_wal_writer(plan.shard_id, config)
        .await
        .map_err(|error| MemWalEnrollmentError::lance("empty shard claim", error))?;
    if writer.shard_id() != plan.shard_id || writer.epoch() != 1 {
        let observed_shard = writer.shard_id();
        let observed_epoch = writer.epoch();
        let _ = writer.close().await;
        return Err(MemWalEnrollmentError::physical(format!(
            "shard claim returned identity {observed_shard} epoch {observed_epoch}, expected {} epoch 1",
            plan.shard_id
        )));
    }
    writer
        .check_fenced()
        .await
        .map_err(|error| MemWalEnrollmentError::lance("empty shard fence check", error))?;
    writer
        .close()
        .await
        .map_err(|error| MemWalEnrollmentError::lance("empty shard close", error))?;

    match classify_enrollment(anchor, before, plan).await? {
        MemWalEnrollmentState::ExactIndexAndExpectedEmptyShard {
            receipt,
            shard_manifest,
        } => Ok((receipt, shard_manifest)),
        state => Err(MemWalEnrollmentError::physical(format!(
            "shard claim did not produce the exact empty-shard state: {state:?}"
        ))),
    }
}

/// Classify the complete same-table rebind effect chain from the exact prior
/// base HEAD. The only attached table effects admitted are:
///
/// `N --drop old MemWAL index--> N+1 --initialize fresh index--> N+2`.
///
/// Shard objects do not advance the table version. Historical prefixes are
/// compared with authenticated ancestry but never recursively scanned; only
/// the fresh shard is inspected under a hard bound.
pub(crate) async fn classify_rebind(
    anchor: &Dataset,
    before: &CurrentHeadWitness,
    plan: &MemWalRebindPlan,
) -> Result<MemWalRebindState, MemWalEnrollmentError> {
    plan.validate()?;
    require_bounded_profile_table(anchor)?;
    let predecessor = anchor
        .checkout_version(before.table_version)
        .await
        .map_err(|error| MemWalEnrollmentError::lance("rebind predecessor open", error))?;
    let observed_before = capture_current_head_witness(&predecessor).await?;
    if observed_before != *before {
        return Err(MemWalEnrollmentError::authority(format!(
            "rebind predecessor differs from captured authority: expected={before:?}, actual={observed_before:?}"
        )));
    }
    let prior_details = predecessor
        .mem_wal_index_details()
        .await
        .map_err(|error| MemWalEnrollmentError::lance("rebind prior index read", error))?
        .ok_or_else(|| MemWalEnrollmentError::physical("rebind predecessor has no MemWAL index"))?;
    let (_, prior_shard) = validate_stream_config_v3_binding(&prior_details, &plan.prior_binding)
        .map_err(|error| MemWalEnrollmentError::physical(error.to_string()))?;
    if prior_shard != plan.prior_shard_id() {
        return Err(MemWalEnrollmentError::physical(
            "rebind predecessor config and intended prior shard disagree",
        ));
    }
    let prior_indices = predecessor
        .load_indices_by_name(MEM_WAL_INDEX_NAME)
        .await
        .map_err(|error| MemWalEnrollmentError::lance("rebind prior metadata read", error))?;
    let [prior_index] = prior_indices.as_slice() else {
        return Err(MemWalEnrollmentError::physical(format!(
            "rebind predecessor must contain exactly one MemWAL index metadata row, got {}",
            prior_indices.len()
        )));
    };

    let top_level_shards = mem_wal_top_level_shards(anchor).await?;
    let has_drop = predecessor
        .has_successor_version()
        .await
        .map_err(|error| MemWalEnrollmentError::lance("rebind drop successor probe", error))?;
    if !has_drop {
        require_exact_rebind_shards(
            &top_level_shards,
            &plan.retained_shard_ids,
            "rebind no-effect state",
        )?;
        return Ok(MemWalRebindState::ExactNoEffect);
    }

    let drop_version = checked_attached_successor(before.table_version, "rebind index drop")?;
    let dropped = predecessor
        .checkout_version(drop_version)
        .await
        .map_err(|error| MemWalEnrollmentError::lance("rebind dropped-index open", error))?;
    let dropped_head = classify_exact_rebind_drop(&dropped, before, prior_index).await?;
    let has_fresh_index = dropped
        .has_successor_version()
        .await
        .map_err(|error| MemWalEnrollmentError::lance("rebind fresh-index probe", error))?;
    if !has_fresh_index {
        require_exact_rebind_shards(
            &top_level_shards,
            &plan.retained_shard_ids,
            "rebind dropped-index state",
        )?;
        return Ok(MemWalRebindState::ExactIndexDropped { dropped_head });
    }

    let fresh_index_version = checked_attached_successor(drop_version, "rebind fresh index")?;
    let buried_version = checked_attached_successor(fresh_index_version, "rebind buried effect")?;
    let fresh = dropped
        .checkout_version(fresh_index_version)
        .await
        .map_err(|error| MemWalEnrollmentError::lance("rebind fresh-index open", error))?;
    if fresh
        .has_successor_version()
        .await
        .map_err(|error| MemWalEnrollmentError::lance("rebind buried successor probe", error))?
    {
        return Err(MemWalEnrollmentError::authority(format!(
            "rebind table effects are buried by attached version {buried_version}"
        )));
    }
    let receipt =
        classify_exact_index_successor(&fresh, &dropped_head, &plan.next_enrollment).await?;
    if receipt.mem_wal_index_uuid == prior_index.uuid {
        return Err(MemWalEnrollmentError::physical(
            "fresh MemWAL index reused the retired index UUID",
        ));
    }

    if top_level_shards == plan.retained_shard_ids {
        return Ok(MemWalRebindState::ExactFreshIndexOnly {
            dropped_head,
            receipt,
        });
    }
    let terminal_shards = plan.expected_terminal_shards();
    require_exact_rebind_shards(
        &top_level_shards,
        &terminal_shards,
        "rebind fresh-shard state",
    )?;
    let fresh_objects = raw_mem_wal_shard_inventory(anchor, plan.next_enrollment.shard_id).await?;
    match classify_rebind_fresh_shard(&fresh, plan.next_enrollment.shard_id, &fresh_objects).await?
    {
        RebindFreshShardState::Provisioned(shard_manifest) => {
            Ok(MemWalRebindState::ExactFreshIndexAndProvisionedShard {
                dropped_head,
                receipt,
                shard_manifest,
            })
        }
        RebindFreshShardState::ClaimManifestOnly(shard_manifest) => {
            Ok(MemWalRebindState::ExactFreshIndexAndClaimManifestOnly {
                dropped_head,
                receipt,
                shard_manifest,
            })
        }
        RebindFreshShardState::FenceOnlyClaim(evidence) => {
            Ok(MemWalRebindState::ExactFreshIndexAndFenceOnlyClaim {
                dropped_head,
                receipt,
                evidence,
            })
        }
    }
}

/// Retire only the selected singleton MemWAL index. Lance's index-drop commit
/// does not delete any `_mem_wal` objects; the post-effect classifier proves
/// the complete historical prefix set is unchanged.
pub(crate) async fn drop_current_index_from_exact_rebind_no_effect(
    dataset: &mut Dataset,
    before: &CurrentHeadWitness,
    plan: &MemWalRebindPlan,
) -> Result<CurrentHeadWitness, MemWalEnrollmentError> {
    match classify_rebind(dataset, before, plan).await? {
        MemWalRebindState::ExactNoEffect => {}
        state => {
            return Err(MemWalEnrollmentError::physical(format!(
                "rebind index drop requires exact no-effect state, got {state:?}"
            )));
        }
    }
    if dataset.version().version != before.table_version {
        *dataset = dataset
            .checkout_version(before.table_version)
            .await
            .map_err(|error| MemWalEnrollmentError::lance("rebind drop exact reopen", error))?;
    }
    dataset
        .drop_index(MEM_WAL_INDEX_NAME)
        .await
        .map_err(|error| MemWalEnrollmentError::lance("rebind index drop", error))?;
    match classify_rebind(dataset, before, plan).await? {
        MemWalRebindState::ExactIndexDropped { dropped_head } => Ok(dropped_head),
        state => Err(MemWalEnrollmentError::physical(format!(
            "rebind drop did not produce exact dropped-index state: {state:?}"
        ))),
    }
}

/// Initialize the fresh config-v3 singleton only after the old index's exact
/// drop is visible at N+1.
pub(crate) async fn initialize_fresh_index_from_exact_rebind_drop(
    dataset: &mut Dataset,
    before: &CurrentHeadWitness,
    plan: &MemWalRebindPlan,
) -> Result<MemWalEnrollmentReceipt, MemWalEnrollmentError> {
    let dropped_head = match classify_rebind(dataset, before, plan).await? {
        MemWalRebindState::ExactIndexDropped { dropped_head } => dropped_head,
        state => {
            return Err(MemWalEnrollmentError::physical(format!(
                "fresh rebind initialization requires exact dropped-index state, got {state:?}"
            )));
        }
    };
    if dataset.version().version != dropped_head.table_version {
        *dataset = dataset
            .checkout_version(dropped_head.table_version)
            .await
            .map_err(|error| {
                MemWalEnrollmentError::lance("rebind initializer exact reopen", error)
            })?;
    }
    let mut builder = dataset.initialize_mem_wal().unsharded();
    for (key, value) in plan.next_enrollment.expected_writer_defaults() {
        builder = builder.add_writer_config_default(key, value);
    }
    builder
        .execute()
        .await
        .map_err(|error| MemWalEnrollmentError::lance("rebind index initialization", error))?;
    match classify_rebind(dataset, before, plan).await? {
        MemWalRebindState::ExactFreshIndexOnly { receipt, .. } => Ok(receipt),
        state => Err(MemWalEnrollmentError::physical(format!(
            "rebind initializer did not produce exact fresh-index state: {state:?}"
        ))),
    }
}

/// Provision the fresh shard, then obtain its first durable fence-only claim.
///
/// Lance's epoch-1 open creates only a manifest because there is no predecessor
/// to fence. A second no-put open claims epoch 2 (or the next epoch after a
/// manifest-only lost attempt) and durably writes the required empty sentinel.
/// Recovery-v18 arms the complete forward-only plan before entering this
/// adapter. Reclassification before every open prevents data-bearing or foreign
/// state from being adopted.
pub(crate) async fn provision_rebind_shard_from_exact_index_only(
    anchor: &Dataset,
    before: &CurrentHeadWitness,
    plan: &MemWalRebindPlan,
) -> Result<(MemWalEnrollmentReceipt, MemWalRebindFenceEvidence), MemWalEnrollmentError> {
    for _ in 0..3 {
        let state = classify_rebind(anchor, before, plan).await?;
        let (receipt, prior_epoch) = match state {
            MemWalRebindState::ExactFreshIndexOnly { receipt, .. } => (receipt, 0),
            MemWalRebindState::ExactFreshIndexAndProvisionedShard {
                receipt,
                shard_manifest,
                ..
            }
            | MemWalRebindState::ExactFreshIndexAndClaimManifestOnly {
                receipt,
                shard_manifest,
                ..
            } => (receipt, shard_manifest.writer_epoch),
            MemWalRebindState::ExactFreshIndexAndFenceOnlyClaim {
                receipt, evidence, ..
            } => return Ok((receipt, evidence)),
            state => {
                return Err(MemWalEnrollmentError::physical(format!(
                    "rebind shard provisioning requires fresh-index or incomplete-claim state, got {state:?}"
                )));
            }
        };
        let fresh = anchor
            .checkout_version(receipt.head.table_version)
            .await
            .map_err(|error| MemWalEnrollmentError::lance("rebind fresh-index reopen", error))?;
        let details = fresh
            .mem_wal_index_details()
            .await
            .map_err(|error| MemWalEnrollmentError::lance("rebind writer defaults read", error))?
            .ok_or_else(|| MemWalEnrollmentError::physical("fresh rebind index disappeared"))?;
        let config = plan.next_enrollment.reconstruct_writer_config(&details)?;
        let writer = fresh
            .mem_wal_writer(plan.next_enrollment.shard_id, config)
            .await
            .map_err(|error| {
                MemWalEnrollmentError::lance("rebind fence-only shard claim", error)
            })?;
        let expected_epoch = prior_epoch
            .checked_add(1)
            .ok_or_else(|| MemWalEnrollmentError::physical("rebind writer epoch overflow"))?;
        if writer.shard_id() != plan.next_enrollment.shard_id || writer.epoch() != expected_epoch {
            let observed_shard = writer.shard_id();
            let observed_epoch = writer.epoch();
            let _ = writer.close().await;
            return Err(MemWalEnrollmentError::physical(format!(
                "rebind claim returned shard {observed_shard} epoch {observed_epoch}, expected {} epoch {expected_epoch}",
                plan.next_enrollment.shard_id
            )));
        }
        writer
            .check_fenced()
            .await
            .map_err(|error| MemWalEnrollmentError::lance("rebind fence check", error))?;
        writer
            .close()
            .await
            .map_err(|error| MemWalEnrollmentError::lance("rebind shard close", error))?;
    }
    match classify_rebind(anchor, before, plan).await? {
        MemWalRebindState::ExactFreshIndexAndFenceOnlyClaim {
            receipt, evidence, ..
        } => Ok((receipt, evidence)),
        state => Err(MemWalEnrollmentError::physical(format!(
            "rebind claim did not converge to exact fence-only state: {state:?}"
        ))),
    }
}

/// Fail closed when a table with no lifecycle authority already carries the
/// singleton MemWAL system index. A covered enrollment gap is resolved (or
/// refused in read-only mode) before this check; an uncovered index is partial
/// format state and must never be adopted by compatibility.
pub(crate) async fn validate_phase_a_stream_absent(
    dataset: &Dataset,
) -> Result<(), MemWalEnrollmentError> {
    require_bounded_profile_table(dataset)?;
    if dataset
        .mem_wal_index_details()
        .await
        .map_err(|error| MemWalEnrollmentError::lance("format consistency index read", error))?
        .is_some()
    {
        return Err(MemWalEnrollmentError::physical(
            "table has a MemWAL index but no durable stream lifecycle authority",
        ));
    }
    let inventory = raw_mem_wal_inventory(dataset).await?;
    if !inventory.is_empty() {
        return Err(MemWalEnrollmentError::physical(format!(
            "table has MemWAL shard artifacts but no durable stream lifecycle authority: {inventory:?}"
        )));
    }
    Ok(())
}

fn require_main(dataset: &Dataset) -> Result<(), MemWalEnrollmentError> {
    let branch = dataset.branch_location().branch.clone();
    if branch.is_some() {
        return Err(MemWalEnrollmentError::UnsupportedBranch { branch });
    }
    Ok(())
}

fn require_bounded_profile_table(dataset: &Dataset) -> Result<(), MemWalEnrollmentError> {
    require_main(dataset)?;
    super::exact_id_primary_key_field_id(dataset, "MemWAL enrollment").map_err(|error| {
        MemWalEnrollmentError::UnsupportedTable {
            reason: error.to_string(),
        }
    })?;
    Ok(())
}

/// Verify `before` again at exact `N`, probe only its immediate attached
/// successor, then prove `N+2` absent from an exact `N+1` handle.
async fn exact_enrollment_successor_dataset(
    anchor: &Dataset,
    before: &CurrentHeadWitness,
) -> Result<Option<Dataset>, MemWalEnrollmentError> {
    let successor_version = before
        .table_version
        .checked_add(1)
        .ok_or_else(|| MemWalEnrollmentError::authority("baseline has no successor version"))?;
    if lance_table::format::is_detached_version(successor_version) {
        return Err(MemWalEnrollmentError::authority(
            "baseline crosses the detached-version boundary",
        ));
    }
    let predecessor = anchor
        .checkout_version(before.table_version)
        .await
        .map_err(|error| MemWalEnrollmentError::lance("exact predecessor open", error))?;
    let observed_before = capture_current_head_witness(&predecessor).await?;
    if observed_before != *before {
        return Err(MemWalEnrollmentError::authority(format!(
            "exact predecessor does not match the captured witness: expected={before:?}, actual={observed_before:?}"
        )));
    }
    if predecessor
        .mem_wal_index_details()
        .await
        .map_err(|error| MemWalEnrollmentError::lance("predecessor index read", error))?
        .is_some()
    {
        return Err(MemWalEnrollmentError::authority(
            "captured predecessor now contains a MemWAL index",
        ));
    }
    let has_successor = predecessor
        .has_successor_version()
        .await
        .map_err(|error| MemWalEnrollmentError::lance("immediate successor probe", error))?;
    if !has_successor {
        return Ok(None);
    }

    let buried_version = successor_version.checked_add(1).ok_or_else(|| {
        MemWalEnrollmentError::authority("enrollment successor has no successor version")
    })?;
    if lance_table::format::is_detached_version(buried_version) {
        return Err(MemWalEnrollmentError::authority(
            "enrollment successor crosses the detached-version boundary",
        ));
    }
    let successor = predecessor
        .checkout_version(successor_version)
        .await
        .map_err(|error| MemWalEnrollmentError::lance("exact successor open", error))?;
    if successor
        .has_successor_version()
        .await
        .map_err(|error| MemWalEnrollmentError::lance("buried successor probe", error))?
    {
        return Err(MemWalEnrollmentError::authority(format!(
            "enrollment effect is buried by attached version {buried_version}"
        )));
    }
    Ok(Some(successor))
}

async fn classify_exact_index_successor(
    successor: &Dataset,
    before: &CurrentHeadWitness,
    plan: &MemWalEnrollmentPlan,
) -> Result<MemWalEnrollmentReceipt, MemWalEnrollmentError> {
    let expected_version = before
        .table_version
        .checked_add(1)
        .ok_or_else(|| MemWalEnrollmentError::authority("baseline has no successor version"))?;
    if successor.version().version != expected_version {
        return Err(MemWalEnrollmentError::authority(format!(
            "enrollment effect is not exact N+1: expected {expected_version}, got {}",
            successor.version().version
        )));
    }
    let head = capture_current_head_witness(successor).await?;
    if head.branch_identifier != before.branch_identifier {
        return Err(MemWalEnrollmentError::authority(
            "branch/ref incarnation changed during enrollment",
        ));
    }
    if before.manifest_e_tag.is_some() && head.manifest_e_tag.is_none() {
        return Err(MemWalEnrollmentError::authority(
            "enrollment successor lost the manifest e_tag",
        ));
    }
    if before.manifest_e_tag.is_some() && head.manifest_e_tag == before.manifest_e_tag {
        return Err(MemWalEnrollmentError::authority(
            "enrollment successor reused the predecessor manifest e_tag",
        ));
    }

    let transaction = successor
        .read_transaction()
        .await
        .map_err(|error| MemWalEnrollmentError::lance("successor transaction read", error))?
        .ok_or_else(|| MemWalEnrollmentError::physical("successor has no transaction"))?;
    if transaction.read_version != before.table_version
        || transaction.uuid.is_empty()
        || transaction.uuid == before.transaction_uuid
        || transaction.uuid != head.transaction_uuid
        || transaction.tag.is_some()
        || transaction.transaction_properties.is_some()
    {
        return Err(MemWalEnrollmentError::physical(format!(
            "successor transaction is not the initializer's exact transaction shape: {transaction:?}"
        )));
    }
    let (new_indices, removed_indices) = match &transaction.operation {
        Operation::CreateIndex {
            new_indices,
            removed_indices,
        } => (new_indices, removed_indices),
        operation => {
            return Err(MemWalEnrollmentError::physical(format!(
                "successor operation is not CreateIndex: {operation:?}"
            )));
        }
    };
    if new_indices.len() != 1 || !removed_indices.is_empty() {
        return Err(MemWalEnrollmentError::physical(
            "initializer transaction is not one index add with zero removals",
        ));
    }
    let index = &new_indices[0];
    if index.name != MEM_WAL_INDEX_NAME
        || index.dataset_version != before.table_version
        || !index.fields.is_empty()
        || index.fragment_bitmap.is_some()
        || index.index_details.is_none()
        || index.index_version != 0
        || index.created_at.is_none()
        || index.base_id.is_some()
        || index.files.is_some()
    {
        return Err(MemWalEnrollmentError::physical(format!(
            "initializer index metadata is not the exact inline singleton shape: {index:?}"
        )));
    }
    let details = successor
        .mem_wal_index_details()
        .await
        .map_err(|error| MemWalEnrollmentError::lance("successor MemWAL details read", error))?
        .ok_or_else(|| MemWalEnrollmentError::physical("successor has no MemWAL details"))?;
    validate_fresh_unsharded_details(&details, plan)?;

    Ok(MemWalEnrollmentReceipt {
        head,
        mem_wal_index_uuid: index.uuid,
    })
}

fn checked_attached_successor(
    version: u64,
    operation: &'static str,
) -> Result<u64, MemWalEnrollmentError> {
    let successor = version
        .checked_add(1)
        .ok_or_else(|| MemWalEnrollmentError::authority(format!("{operation} version overflow")))?;
    if lance_table::format::is_detached_version(successor) {
        return Err(MemWalEnrollmentError::authority(format!(
            "{operation} crosses the detached-version boundary"
        )));
    }
    Ok(successor)
}

async fn classify_exact_rebind_drop(
    dropped: &Dataset,
    before: &CurrentHeadWitness,
    prior_index: &lance_table::format::IndexMetadata,
) -> Result<CurrentHeadWitness, MemWalEnrollmentError> {
    let expected_version = checked_attached_successor(before.table_version, "rebind index drop")?;
    if dropped.version().version != expected_version {
        return Err(MemWalEnrollmentError::authority(format!(
            "rebind drop is not exact N+1: expected {expected_version}, got {}",
            dropped.version().version
        )));
    }
    let head = capture_current_head_witness(dropped).await?;
    validate_rebind_successor_head(before, &head, "index drop")?;
    let transaction = dropped
        .read_transaction()
        .await
        .map_err(|error| MemWalEnrollmentError::lance("rebind drop transaction read", error))?
        .ok_or_else(|| MemWalEnrollmentError::physical("rebind drop has no transaction"))?;
    if transaction.read_version != before.table_version
        || transaction.uuid.is_empty()
        || transaction.uuid == before.transaction_uuid
        || transaction.uuid != head.transaction_uuid
        || transaction.tag.is_some()
        || transaction.transaction_properties.is_some()
    {
        return Err(MemWalEnrollmentError::physical(format!(
            "rebind drop transaction has a foreign shape: {transaction:?}"
        )));
    }
    let (new_indices, removed_indices) = match &transaction.operation {
        Operation::CreateIndex {
            new_indices,
            removed_indices,
        } => (new_indices, removed_indices),
        operation => {
            return Err(MemWalEnrollmentError::physical(format!(
                "rebind drop operation is not CreateIndex: {operation:?}"
            )));
        }
    };
    if !new_indices.is_empty()
        || removed_indices.len() != 1
        || removed_indices.first() != Some(prior_index)
    {
        return Err(MemWalEnrollmentError::physical(format!(
            "rebind drop must remove exactly the selected MemWAL metadata row: new={new_indices:?}, removed={removed_indices:?}"
        )));
    }
    if dropped
        .mem_wal_index_details()
        .await
        .map_err(|error| MemWalEnrollmentError::lance("rebind dropped details read", error))?
        .is_some()
        || !dropped
            .load_indices_by_name(MEM_WAL_INDEX_NAME)
            .await
            .map_err(|error| MemWalEnrollmentError::lance("rebind dropped metadata read", error))?
            .is_empty()
    {
        return Err(MemWalEnrollmentError::physical(
            "rebind drop successor still contains MemWAL index authority",
        ));
    }
    Ok(head)
}

fn validate_rebind_successor_head(
    before: &CurrentHeadWitness,
    after: &CurrentHeadWitness,
    operation: &'static str,
) -> Result<(), MemWalEnrollmentError> {
    if after.branch_identifier != before.branch_identifier {
        return Err(MemWalEnrollmentError::authority(format!(
            "branch/ref incarnation changed during rebind {operation}"
        )));
    }
    if before.manifest_e_tag.is_some() && after.manifest_e_tag.is_none() {
        return Err(MemWalEnrollmentError::authority(format!(
            "rebind {operation} successor lost the manifest e_tag"
        )));
    }
    if before.manifest_e_tag.is_some() && after.manifest_e_tag == before.manifest_e_tag {
        return Err(MemWalEnrollmentError::authority(format!(
            "rebind {operation} successor reused its predecessor manifest e_tag"
        )));
    }
    Ok(())
}

fn require_exact_rebind_shards(
    actual: &[ShardId],
    expected: &[ShardId],
    state: &'static str,
) -> Result<(), MemWalEnrollmentError> {
    if actual != expected {
        return Err(MemWalEnrollmentError::physical(format!(
            "{state} has foreign or missing MemWAL prefixes: expected={expected:?}, actual={actual:?}"
        )));
    }
    Ok(())
}

async fn mem_wal_top_level_shards(
    dataset: &Dataset,
) -> Result<Vec<ShardId>, MemWalEnrollmentError> {
    let object_store = dataset
        .object_store(None)
        .await
        .map_err(|error| MemWalEnrollmentError::lance("rebind root store open", error))?;
    let root = dataset.branch_location().path.join("_mem_wal");
    let listed = object_store
        .inner
        .list_with_delimiter(Some(&root))
        .await
        .map_err(|error| MemWalEnrollmentError::lance("rebind root listing", error))?;
    if !listed.objects.is_empty() {
        return Err(MemWalEnrollmentError::physical(format!(
            "rebind MemWAL root contains loose objects: {:?}",
            listed
                .objects
                .iter()
                .map(|object| object.location.as_ref())
                .collect::<Vec<_>>()
        )));
    }
    if listed.common_prefixes.len() > MAX_RETAINED_REBIND_SHARDS.saturating_add(1) {
        return Err(MemWalEnrollmentError::InventoryLimitExceeded {
            scope: format!("{}/_mem_wal shard prefixes", dataset.branch_location().uri),
            limit: MAX_RETAINED_REBIND_SHARDS.saturating_add(1),
        });
    }
    let mut shards = Vec::with_capacity(listed.common_prefixes.len());
    for prefix in listed.common_prefixes {
        let relative = relative_object_path(&root, &prefix)?;
        if relative.is_empty() || relative.contains('/') {
            return Err(MemWalEnrollmentError::physical(format!(
                "rebind MemWAL root contains malformed nested prefix {prefix:?}"
            )));
        }
        let shard = ShardId::parse_str(&relative).map_err(|error| {
            MemWalEnrollmentError::physical(format!(
                "rebind MemWAL root contains malformed shard prefix {relative:?}: {error}"
            ))
        })?;
        shards.push(shard);
    }
    shards.sort_unstable();
    if shards.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(MemWalEnrollmentError::physical(
            "rebind MemWAL root returned duplicate shard prefixes",
        ));
    }
    Ok(shards)
}

async fn raw_mem_wal_shard_inventory(
    dataset: &Dataset,
    shard_id: ShardId,
) -> Result<Vec<String>, MemWalEnrollmentError> {
    let object_store = dataset
        .object_store(None)
        .await
        .map_err(|error| MemWalEnrollmentError::lance("rebind shard store open", error))?;
    let root = dataset
        .branch_location()
        .path
        .join("_mem_wal")
        .join(shard_id.to_string());
    let objects = strict_collect_bounded(
        object_store.inner.list(Some(&root)),
        MAX_REBIND_FRESH_SHARD_OBJECTS,
        format!("{}/_mem_wal/{shard_id}", dataset.branch_location().uri),
    )
    .await?;
    let mut relative = objects
        .into_iter()
        .map(|object| relative_object_path(&root, &object.location))
        .collect::<Result<Vec<_>, _>>()?;
    relative.sort();
    Ok(relative)
}

enum RebindFreshShardState {
    Provisioned(ShardManifest),
    ClaimManifestOnly(ShardManifest),
    FenceOnlyClaim(MemWalRebindFenceEvidence),
}

async fn classify_rebind_fresh_shard(
    dataset: &Dataset,
    shard_id: ShardId,
    objects: &[String],
) -> Result<RebindFreshShardState, MemWalEnrollmentError> {
    let expected_sentinel = format!(
        "wal/{}",
        lance::dataset::mem_wal::util::wal_entry_filename(1)
    );
    let mut manifest_versions = Vec::new();
    let mut saw_hint = false;
    let mut wal_objects = Vec::new();
    let mut unexpected = Vec::new();
    for object in objects {
        match documented_manifest_version(object) {
            Some(Some(version)) => manifest_versions.push(version),
            Some(None) => {
                if saw_hint {
                    return Err(MemWalEnrollmentError::physical(
                        "fresh rebind shard contains duplicate manifest hints",
                    ));
                }
                saw_hint = true;
            }
            None if object.starts_with("wal/") => wal_objects.push(object.clone()),
            None if documented_manifest_staging(object) => {}
            None => unexpected.push(object.clone()),
        }
    }
    manifest_versions.sort_unstable();
    wal_objects.sort();
    unexpected.sort();
    if manifest_versions.is_empty() || !unexpected.is_empty() {
        return Err(MemWalEnrollmentError::physical(format!(
            "fresh rebind shard has incomplete manifests or unexpected objects: manifests={manifest_versions:?}, unexpected={unexpected:?}"
        )));
    }
    let latest_version = *manifest_versions
        .last()
        .expect("non-empty manifest versions checked above");
    let expected_versions = (1..=latest_version).collect::<Vec<_>>();
    if manifest_versions != expected_versions {
        return Err(MemWalEnrollmentError::physical(format!(
            "fresh rebind shard manifest history has gaps: expected={expected_versions:?}, actual={manifest_versions:?}"
        )));
    }

    let object_store = dataset
        .object_store(None)
        .await
        .map_err(|error| MemWalEnrollmentError::lance("rebind manifest store open", error))?;
    let store = ShardManifestStore::new(
        object_store.clone(),
        &dataset.branch_location().path,
        shard_id,
        SHARD_MANIFEST_SCAN_BATCH_SIZE,
    );
    let mut previous_epoch = 0;
    let mut latest_manifest = None;
    for version in expected_versions {
        let manifest = store
            .read_version(version)
            .await
            .map_err(|error| MemWalEnrollmentError::lance("rebind shard manifest read", error))?;
        validate_empty_rebind_shard_manifest(&manifest, shard_id, version, previous_epoch)?;
        previous_epoch = manifest.writer_epoch;
        latest_manifest = Some(manifest);
    }
    let latest_manifest = latest_manifest.expect("non-empty exact history checked above");

    if wal_objects.is_empty() {
        if latest_manifest.wal_entry_position_last_seen != 0 {
            return Err(MemWalEnrollmentError::physical(
                "fresh rebind shard records a WAL cursor without a WAL object",
            ));
        }
        return Ok(if latest_manifest.writer_epoch == 1 {
            RebindFreshShardState::Provisioned(latest_manifest)
        } else {
            RebindFreshShardState::ClaimManifestOnly(latest_manifest)
        });
    }
    if wal_objects != [expected_sentinel] || latest_manifest.writer_epoch < 2 {
        return Err(MemWalEnrollmentError::physical(format!(
            "fresh rebind shard WAL is not exactly one initial fence sentinel: {wal_objects:?}"
        )));
    }
    let tailer = WalTailer::new(
        object_store,
        dataset.branch_location().path.clone(),
        shard_id,
    );
    let sentinel = tailer
        .read_entry(1)
        .await
        .map_err(|error| MemWalEnrollmentError::lance("rebind sentinel read", error))?
        .ok_or_else(|| MemWalEnrollmentError::physical("listed rebind sentinel disappeared"))?;
    if sentinel.shard_id != shard_id
        || sentinel.entry_position != 1
        || sentinel.writer_epoch != latest_manifest.writer_epoch
        || !sentinel.batches.is_empty()
    {
        return Err(MemWalEnrollmentError::physical(format!(
            "fresh rebind WAL endpoint is not an empty current-epoch sentinel: shard={}, position={}, epoch={}, batches={}",
            sentinel.shard_id,
            sentinel.entry_position,
            sentinel.writer_epoch,
            sentinel.batches.len()
        )));
    }
    Ok(RebindFreshShardState::FenceOnlyClaim(
        MemWalRebindFenceEvidence {
            shard_manifest: latest_manifest,
            sentinel_position: sentinel.entry_position,
            sentinel_writer_epoch: sentinel.writer_epoch,
            base_merged_generation: 0,
        },
    ))
}

fn validate_empty_rebind_shard_manifest(
    manifest: &ShardManifest,
    shard_id: ShardId,
    expected_version: u64,
    previous_epoch: u64,
) -> Result<(), MemWalEnrollmentError> {
    let epoch_is_exact = if expected_version == 1 {
        manifest.writer_epoch == 1
    } else {
        manifest.writer_epoch == previous_epoch
            || manifest.writer_epoch == previous_epoch.saturating_add(1)
    };
    if manifest.shard_id != shard_id
        || manifest.version != expected_version
        || manifest.shard_spec_id != UNSHARDED_SPEC_ID
        || !manifest.shard_field_values.is_empty()
        || !epoch_is_exact
        || manifest.replay_after_wal_entry_position != 0
        || manifest.wal_entry_position_last_seen > 1
        || manifest.current_generation != 1
        || !manifest.flushed_generations.is_empty()
        || manifest.status != ShardStatus::Active
    {
        return Err(MemWalEnrollmentError::physical(format!(
            "fresh rebind shard manifest is not an empty claim-only successor: {manifest:?}"
        )));
    }
    Ok(())
}

fn validate_fresh_unsharded_details(
    details: &MemWalIndexDetails,
    plan: &MemWalEnrollmentPlan,
) -> Result<(), MemWalEnrollmentError> {
    if details.snapshot_ts_millis != 0
        || details.num_shards != 1
        || details.inline_snapshots.is_some()
        || details.sharding_specs.len() != 1
        || !details.maintained_indexes.is_empty()
        || !details.merged_generations.is_empty()
        || !details.index_catchup.is_empty()
    {
        return Err(MemWalEnrollmentError::physical(format!(
            "MemWAL details are not a fresh unsharded enrollment: {details:?}"
        )));
    }
    let sharding_spec = &details.sharding_specs[0];
    if sharding_spec.spec_id != UNSHARDED_SPEC_ID || sharding_spec.fields.len() != 1 {
        return Err(MemWalEnrollmentError::physical(
            "fresh enrollment has an unexpected sharding spec",
        ));
    }
    let sharding_field = &sharding_spec.fields[0];
    if sharding_field.field_id != "bucket"
        || !sharding_field.source_ids.is_empty()
        || sharding_field.transform.as_deref() != Some("unsharded")
        || sharding_field.expression.is_some()
        || sharding_field.result_type != "int32"
        || !sharding_field.parameters.is_empty()
    {
        return Err(MemWalEnrollmentError::physical(
            "fresh enrollment has an unexpected unsharded field",
        ));
    }
    let actual = details
        .writer_config_defaults
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    let expected = plan.expected_writer_defaults();
    if actual != expected {
        return Err(MemWalEnrollmentError::physical(format!(
            "persisted enrollment marker/config differs: expected={expected:?}, actual={actual:?}"
        )));
    }
    Ok(())
}

async fn raw_mem_wal_inventory(
    dataset: &Dataset,
) -> Result<RawMemWalInventory, MemWalEnrollmentError> {
    let object_store = dataset
        .object_store(None)
        .await
        .map_err(|error| MemWalEnrollmentError::lance("inventory store open", error))?;
    let root = dataset.branch_location().path.join("_mem_wal");
    let objects = strict_collect_bounded(
        object_store.inner.list(Some(&root)),
        MAX_MEM_WAL_INVENTORY_OBJECTS,
        format!("{}/_mem_wal", dataset.branch_location().uri),
    )
    .await?;
    classify_raw_inventory(&root, objects)
}

fn classify_raw_inventory(
    root: &Path,
    objects: Vec<ObjectMeta>,
) -> Result<RawMemWalInventory, MemWalEnrollmentError> {
    let mut inventory = RawMemWalInventory::default();
    for object in objects {
        let relative = relative_object_path(root, &object.location)?;
        let Some((component, rest)) = relative.split_once('/') else {
            inventory.loose_objects.push(relative);
            continue;
        };
        match ShardId::parse_str(component) {
            Ok(shard_id) if !rest.is_empty() => inventory
                .objects_by_shard
                .entry(shard_id)
                .or_default()
                .push(rest.to_string()),
            _ => {
                inventory.malformed_prefixes.insert(component.to_string());
            }
        }
    }
    for objects in inventory.objects_by_shard.values_mut() {
        objects.sort();
    }
    inventory.loose_objects.sort();
    Ok(inventory)
}

async fn strict_collect_bounded<S, T, E>(
    mut stream: S,
    limit: usize,
    scope: String,
) -> Result<Vec<T>, MemWalEnrollmentError>
where
    S: TryStream<Ok = T, Error = E> + Unpin,
    E: Display,
{
    let mut values = Vec::with_capacity(limit.min(4));
    while let Some(value) = stream
        .try_next()
        .await
        .map_err(|error| MemWalEnrollmentError::lance("strict inventory listing", error))?
    {
        if values.len() == limit {
            return Err(MemWalEnrollmentError::InventoryLimitExceeded { scope, limit });
        }
        values.push(value);
    }
    Ok(values)
}

fn relative_object_path(root: &Path, location: &Path) -> Result<String, MemWalEnrollmentError> {
    let root = root.as_ref().trim_end_matches('/');
    let location = location.as_ref();
    if root.is_empty() {
        return Ok(location.trim_start_matches('/').to_string());
    }
    let prefix = format!("{root}/");
    location
        .strip_prefix(&prefix)
        .map(str::to_string)
        .ok_or_else(|| {
            MemWalEnrollmentError::physical(format!(
                "inventory object {location:?} is outside root {root:?}"
            ))
        })
}

fn documented_manifest_version(relative: &str) -> Option<Option<u64>> {
    if relative == "manifest/version_hint.json" {
        return Some(None);
    }
    let filename = relative.strip_prefix("manifest/")?;
    let bits = filename.strip_suffix(".binpb")?;
    if bits.len() != 64 || !bits.bytes().all(|byte| matches!(byte, b'0' | b'1')) {
        return None;
    }
    let reversed = u64::from_str_radix(bits, 2).ok()?;
    Some(Some(reversed.reverse_bits()))
}

/// Lance may leave a losing atomic-CAS shard-manifest staging object. It never
/// became authority, so exact physical classifiers ignore only this canonical
/// `manifest/<positive-version>.binpb.tmp.<uuid>` shape and fail closed on
/// every other unrecognized object.
fn documented_manifest_staging(relative: &str) -> bool {
    let Some(filename) = relative.strip_prefix("manifest/") else {
        return false;
    };
    let Some((manifest_name, temporary_id)) = filename.split_once(".tmp.") else {
        return false;
    };
    let canonical_manifest = format!("manifest/{manifest_name}");
    if !matches!(
        documented_manifest_version(&canonical_manifest),
        Some(Some(1..))
    ) {
        return false;
    }
    ShardId::parse_str(temporary_id)
        .is_ok_and(|parsed| parsed.as_hyphenated().to_string() == temporary_id)
}

async fn exact_empty_shard_manifest(
    dataset: &Dataset,
    shard_id: ShardId,
    objects: &[String],
) -> Result<ShardManifest, MemWalEnrollmentError> {
    let mut manifest_versions = Vec::new();
    let mut unexpected_objects = Vec::new();
    for object in objects {
        match documented_manifest_version(object) {
            Some(Some(version)) => manifest_versions.push(version),
            Some(None) => {}
            None if documented_manifest_staging(object) => {}
            None => unexpected_objects.push(object.clone()),
        }
    }
    manifest_versions.sort_unstable();
    unexpected_objects.sort();
    if manifest_versions != vec![1] || !unexpected_objects.is_empty() {
        return Err(MemWalEnrollmentError::physical(format!(
            "expected empty shard has manifest versions {manifest_versions:?} and unexpected objects {unexpected_objects:?}"
        )));
    }

    let object_store = dataset
        .object_store(None)
        .await
        .map_err(|error| MemWalEnrollmentError::lance("shard manifest store open", error))?;
    let store = ShardManifestStore::new(
        object_store,
        &dataset.branch_location().path,
        shard_id,
        SHARD_MANIFEST_SCAN_BATCH_SIZE,
    );
    let manifest = store
        .read_version(1)
        .await
        .map_err(|error| MemWalEnrollmentError::lance("exact shard manifest read", error))?;
    validate_exact_fresh_shard_manifest(&manifest, shard_id)?;
    Ok(manifest)
}

fn validate_exact_fresh_shard_manifest(
    manifest: &ShardManifest,
    shard_id: ShardId,
) -> Result<(), MemWalEnrollmentError> {
    if manifest.shard_id != shard_id
        || manifest.version != 1
        || manifest.shard_spec_id != UNSHARDED_SPEC_ID
        || !manifest.shard_field_values.is_empty()
        || manifest.writer_epoch != 1
        || manifest.replay_after_wal_entry_position != 0
        || manifest.wal_entry_position_last_seen != 0
        || manifest.current_generation != 1
        || !manifest.flushed_generations.is_empty()
        || manifest.status != ShardStatus::Active
    {
        return Err(MemWalEnrollmentError::physical(format!(
            "pre-minted shard is not the exact fresh active epoch-1 state: {manifest:?}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use futures::stream;
    use lance::dataset::transaction::{Operation, Transaction, UpdateMap};
    use lance::dataset::{CommitBuilder, WriteMode, WriteParams};
    use lance::datatypes::LANCE_UNENFORCED_PRIMARY_KEY;
    use lance_file::version::LanceFileVersion;

    use super::*;

    async fn fresh_dataset(uri: &str) -> Dataset {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false).with_metadata(HashMap::from([(
                LANCE_UNENFORCED_PRIMARY_KEY.to_string(),
                "true".to_string(),
            )])),
            Field::new("value", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["alice"])),
                Arc::new(Int32Array::from(vec![1])),
            ],
        )
        .unwrap();
        let reader = RecordBatchIterator::new([Ok(batch)], schema);
        // forbidden-api-allow: test-only exact enrollment fixture construction
        Dataset::write(
            reader,
            uri,
            Some(WriteParams {
                mode: WriteMode::Create,
                enable_stable_row_ids: true,
                data_storage_version: Some(LanceFileVersion::V2_2),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
    }

    fn plan() -> MemWalEnrollmentPlan {
        MemWalEnrollmentPlan::new(ShardId::new_v4(), ShardId::new_v4()).unwrap()
    }

    fn binding_for(plan: &MemWalEnrollmentPlan) -> StreamPhysicalBinding {
        StreamPhysicalBinding {
            stable_table_id: 7,
            table_incarnation_id: 9,
            table_location: "nodes/person-7-9.lance".to_string(),
            table_branch: None,
            enrollment_id: plan.enrollment_id.to_string(),
            shard_ids: vec![plan.shard_id.to_string()],
            stream_config_version: STREAM_CONFIG_VERSION,
            stream_config_hash: plan.stream_config_hash(),
        }
    }

    async fn enrolled_dataset(uri: &str) -> (Dataset, MemWalEnrollmentPlan) {
        let mut dataset = fresh_dataset(uri).await;
        let before = capture_pre_enrollment_head(&dataset).await.unwrap();
        let plan = plan();
        initialize_index_from_exact_no_effect(&mut dataset, &before, &plan)
            .await
            .unwrap();
        provision_shard_from_exact_index_only(&dataset, &before, &plan)
            .await
            .unwrap();
        (dataset, plan)
    }

    #[tokio::test]
    async fn adapter_classifies_and_executes_only_the_two_enrollment_effects() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("phase-a-enrollment.lance");
        let uri = uri.to_str().unwrap();
        let mut dataset = fresh_dataset(uri).await;
        let before = capture_pre_enrollment_head(&dataset).await.unwrap();
        let plan = plan();

        assert_eq!(
            classify_enrollment(&dataset, &before, &plan).await.unwrap(),
            MemWalEnrollmentState::ExactNoEffect
        );
        let index_receipt = initialize_index_from_exact_no_effect(&mut dataset, &before, &plan)
            .await
            .unwrap();
        assert_eq!(index_receipt.head.table_version, before.table_version + 1);
        assert!(matches!(
            classify_enrollment(&dataset, &before, &plan).await.unwrap(),
            MemWalEnrollmentState::ExactIndexOnly(_)
        ));

        let (receipt, shard) = provision_shard_from_exact_index_only(&dataset, &before, &plan)
            .await
            .unwrap();
        assert_eq!(receipt, index_receipt);
        assert_eq!(shard.shard_id, plan.shard_id);
        assert_eq!(shard.writer_epoch, 1);
        assert!(matches!(
            classify_enrollment(&dataset, &before, &plan).await.unwrap(),
            MemWalEnrollmentState::ExactIndexAndExpectedEmptyShard { .. }
        ));

        let error = provision_shard_from_exact_index_only(&dataset, &before, &plan)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            MemWalEnrollmentError::PhysicalConflict { .. }
        ));
        let final_state = classify_enrollment(&dataset, &before, &plan).await.unwrap();
        let MemWalEnrollmentState::ExactIndexAndExpectedEmptyShard { shard_manifest, .. } =
            final_state
        else {
            panic!("expected empty shard");
        };
        assert_eq!(
            shard_manifest.writer_epoch, 1,
            "refused re-provisioning must not claim the shard a second time"
        );
    }

    #[tokio::test]
    async fn rebind_drops_and_recreates_only_index_authority_and_retains_old_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("physical-rebind.lance");
        let uri = uri.to_str().unwrap();
        let (mut dataset, old_plan) = enrolled_dataset(uri).await;
        let before = capture_current_head_witness(&dataset).await.unwrap();
        let old_store = ShardManifestStore::new(
            dataset.object_store(None).await.unwrap(),
            &dataset.branch_location().path,
            old_plan.shard_id,
            SHARD_MANIFEST_SCAN_BATCH_SIZE,
        );
        let old_manifest = old_store.read_version(1).await.unwrap();
        let next_plan = plan();
        let rebind = MemWalRebindPlan::new(
            binding_for(&old_plan),
            vec![old_plan.shard_id],
            next_plan.clone(),
        )
        .unwrap();

        assert_eq!(
            classify_rebind(&dataset, &before, &rebind).await.unwrap(),
            MemWalRebindState::ExactNoEffect
        );
        let dropped =
            drop_current_index_from_exact_rebind_no_effect(&mut dataset, &before, &rebind)
                .await
                .unwrap();
        assert_eq!(dropped.table_version, before.table_version + 1);
        assert!(matches!(
            classify_rebind(&dataset, &before, &rebind).await.unwrap(),
            MemWalRebindState::ExactIndexDropped { .. }
        ));

        let receipt = initialize_fresh_index_from_exact_rebind_drop(&mut dataset, &before, &rebind)
            .await
            .unwrap();
        assert_eq!(receipt.head.table_version, before.table_version + 2);
        assert!(matches!(
            classify_rebind(&dataset, &before, &rebind).await.unwrap(),
            MemWalRebindState::ExactFreshIndexOnly { .. }
        ));

        let (terminal_receipt, evidence) =
            provision_rebind_shard_from_exact_index_only(&dataset, &before, &rebind)
                .await
                .unwrap();
        assert_eq!(terminal_receipt, receipt);
        assert_eq!(evidence.sentinel_position, 1);
        assert_eq!(evidence.sentinel_writer_epoch, 2);
        assert_eq!(evidence.shard_manifest.writer_epoch, 2);
        assert_eq!(evidence.shard_manifest.replay_after_wal_entry_position, 0);
        assert_eq!(evidence.shard_manifest.current_generation, 1);
        assert_eq!(evidence.base_merged_generation, 0);
        assert!(matches!(
            classify_rebind(&dataset, &before, &rebind).await.unwrap(),
            MemWalRebindState::ExactFreshIndexAndFenceOnlyClaim { .. }
        ));

        assert_eq!(old_store.read_version(1).await.unwrap(), old_manifest);
        let mut expected_shards = vec![old_plan.shard_id, next_plan.shard_id];
        expected_shards.sort_unstable();
        assert_eq!(
            mem_wal_top_level_shards(&dataset).await.unwrap(),
            expected_shards
        );

        let (_, retried) = provision_rebind_shard_from_exact_index_only(&dataset, &before, &rebind)
            .await
            .unwrap();
        assert_eq!(retried.sentinel_position, 1);
        assert_eq!(retried.sentinel_writer_epoch, 2);
    }

    #[tokio::test]
    async fn rebind_claim_converges_forward_after_manifest_only_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("physical-rebind-manifest-only.lance");
        let uri = uri.to_str().unwrap();
        let (mut dataset, old_plan) = enrolled_dataset(uri).await;
        let before = capture_current_head_witness(&dataset).await.unwrap();
        let rebind =
            MemWalRebindPlan::new(binding_for(&old_plan), vec![old_plan.shard_id], plan()).unwrap();
        drop_current_index_from_exact_rebind_no_effect(&mut dataset, &before, &rebind)
            .await
            .unwrap();
        initialize_fresh_index_from_exact_rebind_drop(&mut dataset, &before, &rebind)
            .await
            .unwrap();

        let store = ShardManifestStore::new(
            dataset.object_store(None).await.unwrap(),
            &dataset.branch_location().path,
            rebind.next_enrollment.shard_id,
            SHARD_MANIFEST_SCAN_BATCH_SIZE,
        );
        // forbidden-api-allow: test-only crash-window construction between
        // Lance's epoch manifest and its automatic fence sentinel.
        assert_eq!(store.claim_epoch(UNSHARDED_SPEC_ID).await.unwrap().0, 1);
        assert_eq!(store.claim_epoch(UNSHARDED_SPEC_ID).await.unwrap().0, 2);
        assert!(matches!(
            classify_rebind(&dataset, &before, &rebind)
                .await
                .unwrap(),
            MemWalRebindState::ExactFreshIndexAndClaimManifestOnly {
                shard_manifest,
                ..
            } if shard_manifest.writer_epoch == 2
        ));

        let (_, evidence) =
            provision_rebind_shard_from_exact_index_only(&dataset, &before, &rebind)
                .await
                .unwrap();
        assert_eq!(evidence.sentinel_position, 1);
        assert_eq!(evidence.sentinel_writer_epoch, 3);
        assert_eq!(evidence.shard_manifest.writer_epoch, 3);
    }

    #[test]
    fn rebind_classifier_accepts_only_canonical_lance_manifest_staging() {
        let bits = format!("{:064b}", 1_u64.reverse_bits());
        let temporary_id = "01234567-89ab-cdef-0123-456789abcdef";
        assert!(documented_manifest_staging(&format!(
            "manifest/{bits}.binpb.tmp.{temporary_id}"
        )));
        for malformed in [
            format!("manifest/{bits}.binpb"),
            format!("manifest/{bits}.binpb.tmp.NOT-A-UUID"),
            format!(
                "manifest/{bits}.binpb.tmp.{}",
                temporary_id.to_ascii_uppercase()
            ),
            format!("manifest/{}.binpb.tmp.{temporary_id}", "0".repeat(64)),
            format!("wal/{bits}.arrow.tmp.{temporary_id}"),
        ] {
            assert!(
                !documented_manifest_staging(&malformed),
                "malformed staging path must fail closed: {malformed}"
            );
        }
    }

    #[tokio::test]
    async fn rebind_classifier_rejects_foreign_drop_burial_and_shard_prefixes() {
        let dir = tempfile::tempdir().unwrap();

        let foreign_uri = dir.path().join("rebind-foreign-drop.lance");
        let foreign_uri = foreign_uri.to_str().unwrap();
        let (mut foreign, old_plan) = enrolled_dataset(foreign_uri).await;
        let before = capture_current_head_witness(&foreign).await.unwrap();
        let rebind =
            MemWalRebindPlan::new(binding_for(&old_plan), vec![old_plan.shard_id], plan()).unwrap();
        let transaction = Transaction::new(
            foreign.version().version,
            Operation::UpdateConfig {
                config_updates: Some(UpdateMap {
                    update_entries: vec![
                        (
                            "omnigraph.test.foreign-rebind".to_string(),
                            Some("true".to_string()),
                        )
                            .into(),
                    ],
                    replace: false,
                }),
                table_metadata_updates: None,
                schema_metadata_updates: None,
                field_metadata_updates: HashMap::new(),
            },
            None,
        );
        // forbidden-api-allow: test-only foreign N+1 classifier evidence
        foreign = CommitBuilder::new(Arc::new(foreign))
            .execute(transaction)
            .await
            .unwrap();
        let error = classify_rebind(&foreign, &before, &rebind)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            MemWalEnrollmentError::PhysicalConflict { .. }
        ));

        let buried_uri = dir.path().join("rebind-buried.lance");
        let buried_uri = buried_uri.to_str().unwrap();
        let (mut buried, old_plan) = enrolled_dataset(buried_uri).await;
        let before = capture_current_head_witness(&buried).await.unwrap();
        let rebind =
            MemWalRebindPlan::new(binding_for(&old_plan), vec![old_plan.shard_id], plan()).unwrap();
        drop_current_index_from_exact_rebind_no_effect(&mut buried, &before, &rebind)
            .await
            .unwrap();
        initialize_fresh_index_from_exact_rebind_drop(&mut buried, &before, &rebind)
            .await
            .unwrap();
        let transaction = Transaction::new(
            buried.version().version,
            Operation::UpdateConfig {
                config_updates: Some(UpdateMap {
                    update_entries: vec![
                        (
                            "omnigraph.test.buried-rebind".to_string(),
                            Some("true".to_string()),
                        )
                            .into(),
                    ],
                    replace: false,
                }),
                table_metadata_updates: None,
                schema_metadata_updates: None,
                field_metadata_updates: HashMap::new(),
            },
            None,
        );
        // forbidden-api-allow: test-only buried N+3 classifier evidence
        buried = CommitBuilder::new(Arc::new(buried))
            .execute(transaction)
            .await
            .unwrap();
        let error = classify_rebind(&buried, &before, &rebind)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            MemWalEnrollmentError::AuthorityChanged { .. }
        ));
        assert!(error.to_string().contains("buried"));

        let prefix_uri = dir.path().join("rebind-foreign-prefix.lance");
        let prefix_uri = prefix_uri.to_str().unwrap();
        let (mut prefix, old_plan) = enrolled_dataset(prefix_uri).await;
        let before = capture_current_head_witness(&prefix).await.unwrap();
        let rebind =
            MemWalRebindPlan::new(binding_for(&old_plan), vec![old_plan.shard_id], plan()).unwrap();
        drop_current_index_from_exact_rebind_no_effect(&mut prefix, &before, &rebind)
            .await
            .unwrap();
        initialize_fresh_index_from_exact_rebind_drop(&mut prefix, &before, &rebind)
            .await
            .unwrap();
        let details = prefix.mem_wal_index_details().await.unwrap().unwrap();
        let foreign_shard = ShardId::new_v4();
        let config = reconstruct_b1_writer_config(
            &details,
            rebind.next_enrollment.enrollment_id,
            foreign_shard,
        )
        .unwrap();
        let writer = prefix.mem_wal_writer(foreign_shard, config).await.unwrap();
        writer.close().await.unwrap();
        let error = classify_rebind(&prefix, &before, &rebind)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            MemWalEnrollmentError::PhysicalConflict { .. }
        ));
        assert!(error.to_string().contains("foreign or missing"));
    }

    #[test]
    fn rebind_plan_requires_canonical_authenticated_shard_ancestry() {
        let old = plan();
        let next = plan();
        let another = ShardId::new_v4();
        let mut retained = vec![old.shard_id, another];
        retained.sort_unstable();
        assert!(MemWalRebindPlan::new(binding_for(&old), retained.clone(), next.clone()).is_ok());

        retained.reverse();
        assert!(matches!(
            MemWalRebindPlan::new(binding_for(&old), retained, next.clone()).unwrap_err(),
            MemWalEnrollmentError::InvalidPlan { .. }
        ));
        assert!(matches!(
            MemWalRebindPlan::new(binding_for(&old), vec![old.shard_id], old).unwrap_err(),
            MemWalEnrollmentError::InvalidPlan { .. }
        ));
    }

    #[tokio::test]
    async fn classifier_rejects_an_effect_buried_after_exact_n_plus_one() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("buried-enrollment.lance");
        let uri = uri.to_str().unwrap();
        let mut dataset = fresh_dataset(uri).await;
        let before = capture_pre_enrollment_head(&dataset).await.unwrap();
        let plan = plan();
        initialize_index_from_exact_no_effect(&mut dataset, &before, &plan)
            .await
            .unwrap();

        let transaction = Transaction::new(
            dataset.version().version,
            Operation::UpdateConfig {
                config_updates: Some(UpdateMap {
                    update_entries: vec![
                        (
                            "omnigraph.test.buried".to_string(),
                            Some("true".to_string()),
                        )
                            .into(),
                    ],
                    replace: false,
                }),
                table_metadata_updates: None,
                schema_metadata_updates: None,
                field_metadata_updates: HashMap::new(),
            },
            None,
        );
        // forbidden-api-allow: test-only foreign successor used to prove buried-effect refusal
        dataset = CommitBuilder::new(Arc::new(dataset))
            .execute(transaction)
            .await
            .unwrap();

        let error = classify_enrollment(&dataset, &before, &plan)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            MemWalEnrollmentError::AuthorityChanged { .. }
        ));
        assert!(error.to_string().contains("buried by attached version"));
    }

    #[tokio::test]
    async fn bounded_inventory_fails_instead_of_returning_a_partial_prefix() {
        let values = (0_u8..=3).map(Ok::<_, &'static str>);
        let error = strict_collect_bounded(stream::iter(values), 3, "test".to_string())
            .await
            .unwrap_err();
        assert_eq!(
            error,
            MemWalEnrollmentError::InventoryLimitExceeded {
                scope: "test".to_string(),
                limit: 3,
            }
        );

        let partial_error = strict_collect_bounded(
            stream::iter(vec![Ok::<_, &'static str>(1_u8), Err("page failed")]),
            3,
            "test".to_string(),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            partial_error,
            MemWalEnrollmentError::Lance {
                operation: "strict inventory listing",
                ..
            }
        ));
    }

    #[test]
    fn exact_table_version_classifier_has_no_latest_or_history_listing_escape() {
        let source = include_str!("mem_wal.rs");
        let start = source
            .find("async fn exact_enrollment_successor_dataset")
            .expect("exact classifier exists");
        let end = source[start..]
            .find("async fn classify_exact_index_successor")
            .map(|offset| start + offset)
            .expect("exact classifier boundary exists");
        let classifier = &source[start..end];
        for forbidden in [
            ".checkout_latest(",
            ".latest_version_id(",
            ".is_stale(",
            ".versions(",
        ] {
            assert!(
                !classifier.contains(forbidden),
                "exact enrollment classifier must not use {forbidden}"
            );
        }
        assert!(classifier.contains(".has_successor_version()"));
        assert!(classifier.contains(".checkout_version(before.table_version)"));
        assert!(classifier.contains(".checkout_version(successor_version)"));
    }

    #[test]
    fn phase_a_adapter_has_no_row_or_raw_object_mutation_escape() {
        let source = include_str!("mem_wal.rs");
        let production = source
            .split("#[cfg(test)]")
            .next()
            .expect("production module precedes its tests");
        for forbidden in [
            ".put(",
            ".put_no_wait(",
            ".append(",
            ".delete(",
            "mark_generations_as_merged",
        ] {
            assert!(
                !production.contains(forbidden),
                "Phase A enrollment must not expose row admission, folding, or raw deletion through {forbidden}"
            );
        }
        assert!(production.contains(".initialize_mem_wal()"));
        assert!(production.contains(".mem_wal_writer("));
        assert!(production.contains(".read_version(1)"));
    }

    #[test]
    fn writer_config_is_reconstructed_only_from_exact_persisted_defaults() {
        let plan = plan();
        assert_eq!(
            plan.stream_config_hash(),
            "sha256:5cc5116580e888cbf1b39e8f6be9c514a9a4b8eb6b8da804d05e40678d24f51a"
        );
        let mut details = MemWalIndexDetails {
            num_shards: 1,
            sharding_specs: vec![lance_index::mem_wal::ShardingSpec {
                spec_id: UNSHARDED_SPEC_ID,
                fields: vec![lance_index::mem_wal::ShardingField {
                    field_id: "bucket".to_string(),
                    source_ids: Vec::new(),
                    transform: Some("unsharded".to_string()),
                    expression: None,
                    result_type: "int32".to_string(),
                    parameters: HashMap::new(),
                }],
            }],
            writer_config_defaults: plan.expected_writer_defaults().into_iter().collect(),
            ..Default::default()
        };
        let config = plan.reconstruct_writer_config(&details).unwrap();
        assert_eq!(config.shard_id, plan.shard_id);
        assert_eq!(config.shard_spec_id, UNSHARDED_SPEC_ID);
        assert!(config.durable_write);
        assert_eq!(config.max_wal_buffer_size, 10 * 1024 * 1024);
        assert_eq!(config.max_memtable_rows, 8_193);
        assert_eq!(config.max_memtable_batches, 8_193);

        details
            .writer_config_defaults
            .insert("foreign.default".to_string(), "1".to_string());
        assert!(matches!(
            plan.reconstruct_writer_config(&details).unwrap_err(),
            MemWalEnrollmentError::PhysicalConflict { .. }
        ));
    }
}
