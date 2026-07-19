//! Narrow RFC-026 adapters for bounded MemWAL enrollment and private Phase B1.
//!
//! The parent module captures the exact main-branch witness, initializes the
//! singleton unsharded MemWAL index, provisions one pre-minted empty shard, and
//! classifies those enrollment effects after a lost result. The private
//! [`worker`] submodule owns B1's one-generation admission, watcher, replay,
//! seal/drain, and quiesced retirement mechanics. Graph authority, recovery-v11
//! fold ownership, and the sole `__manifest` visibility publication remain in
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
use lance::dataset::mem_wal::{DatasetMemWalExt, ShardManifestStore, ShardWriterConfig};
use lance::dataset::transaction::Operation;
use lance_index::mem_wal::{
    MEM_WAL_INDEX_NAME, MemWalIndexDetails, ShardId, ShardManifest, ShardStatus,
};
use object_store::{ObjectMeta, path::Path};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::db::manifest::CurrentHeadWitness;

const UNSHARDED_SPEC_ID: u32 = 1;
const SHARD_MANIFEST_SCAN_BATCH_SIZE: usize = 2;
/// A valid fresh shard has two objects (manifest v1 plus its optional hint).
/// Eight leaves enough room to describe a small corrupt/foreign inventory while
/// placing a hard ceiling on evidence collection.  More state is ambiguous, not
/// permission to keep scanning an unbounded prefix.
const MAX_MEM_WAL_INVENTORY_OBJECTS: usize = 8;

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
        stream_config_v2_hash()
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
            "sha256:1885f9b7d28ffc12266e75e3d6d0448c3289dee152674aadb8cca2be865d8e9d"
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
