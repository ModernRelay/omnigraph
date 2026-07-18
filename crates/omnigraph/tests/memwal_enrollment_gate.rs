//! RFC-026 Gate E0: production-neutral evidence for recoverable MemWAL enrollment.
//!
//! This file deliberately uses only public Lance RC.1 APIs. It does not add a
//! production writer, graph format, sidecar, or recovery path. Instead, it asks
//! whether an opaque `initialize_mem_wal().execute()` effect can be classified
//! after a lost acknowledgement as exactly the one allowed successor of a
//! captured dataset HEAD.
//!
//! The evidence is intentionally bounded:
//! - the observed receipt is reconstructed after the initializer commits; Lance
//!   still does not return a caller-owned enrollment receipt;
//! - the durable enrollment marker lives in the MemWAL index's persisted writer
//!   defaults and is checked independently of the replaceable index UUID;
//! - shard sealing/reopening is a second public effect, not atomic with index
//!   enrollment. The lifecycle probe below proves the primitive, not a combined
//!   enrollment transaction.

mod helpers;

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::{TryStream, TryStreamExt};
use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::mem_wal::{
    DatasetMemWalExt, ShardManifestStore, ShardWriter, ShardWriterConfig,
};
use lance::dataset::refs::BranchIdentifier;
use lance::dataset::transaction::{Operation, Transaction, TransactionBuilder, UpdateMap};
use lance::dataset::{
    CommitBuilder, MergeInsertBuilder, WhenMatched, WhenNotMatched, WriteMode, WriteParams,
};
use lance::datatypes::LANCE_UNENFORCED_PRIMARY_KEY;
use lance::index::DatasetIndexExt;
use lance::session::Session;
use lance_file::version::LanceFileVersion;
use lance_index::IndexType;
use lance_index::mem_wal::{
    FlushedGeneration, MEM_WAL_INDEX_NAME, MergedGeneration, ShardId, ShardStatus,
};
use lance_index::scalar::ScalarIndexParams;
use object_store::{ObjectStoreExt, path::Path};

use helpers::cost::{
    AttemptOutcome, AttemptTracker, open_attempt_tracked_lance_dataset_at_version,
};

const ENROLLMENT_ID_KEY: &str = "omnigraph.enrollment_id";
const CONFIG_VERSION_KEY: &str = "omnigraph.stream_config_version";
const CONFIG_VERSION: &str = "1";
const WAL_BUFFER_BYTES: usize = 1_048_576;

#[derive(Debug, Clone, PartialEq, Eq)]
struct PreEnrollmentHead {
    version: u64,
    branch_identifier: BranchIdentifier,
    transaction_uuid: String,
    manifest_e_tag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedEnrollmentReceipt {
    version: u64,
    transaction_uuid: String,
    mem_wal_index_uuid: ShardId,
    manifest_e_tag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CombinedEnrollmentState {
    ExactNoEffect,
    ExactIndexOnly(ObservedEnrollmentReceipt),
    ExactIndexAndExpectedEmptyShard(ObservedEnrollmentReceipt),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RawMemWalRootInventory {
    shard_ids: Vec<ShardId>,
    malformed_prefixes: Vec<String>,
    loose_objects: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StrictShardObjectInventory {
    manifest_versions: Vec<u64>,
    unexpected_objects: Vec<String>,
}

impl RawMemWalRootInventory {
    fn is_empty(&self) -> bool {
        self.shard_ids.is_empty()
            && self.malformed_prefixes.is_empty()
            && self.loose_objects.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CurrentHeadWitness {
    version: u64,
    branch_identifier: BranchIdentifier,
    transaction_uuid: String,
    manifest_e_tag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ProbeAttemptShape {
    method: &'static str,
    object_kind: &'static str,
    outcome: AttemptOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExactEnrollmentProbeCost {
    attempts: Vec<ProbeAttemptShape>,
}

fn reject_if(condition: bool, message: impl Into<String>) -> Result<(), String> {
    if condition {
        Err(message.into())
    } else {
        Ok(())
    }
}

fn relative_object_path(root: &Path, location: &Path) -> Result<String, String> {
    let root = root.as_ref().trim_end_matches('/');
    let location = location.as_ref();
    if root.is_empty() {
        return Ok(location.trim_start_matches('/').to_string());
    }
    let prefix = format!("{root}/");
    location
        .strip_prefix(&prefix)
        .map(str::to_string)
        .ok_or_else(|| format!("object {location:?} is outside inventory root {root:?}"))
}

fn documented_manifest_version(relative: &str) -> Option<Option<u64>> {
    if relative == "manifest/version_hint.json" {
        return Some(None);
    }
    let Some(filename) = relative.strip_prefix("manifest/") else {
        return None;
    };
    let Some(bits) = filename.strip_suffix(".binpb") else {
        return None;
    };
    if bits.len() != 64 || !bits.bytes().all(|byte| matches!(byte, b'0' | b'1')) {
        return None;
    }
    let reversed = u64::from_str_radix(bits, 2).ok()?;
    Some(Some(reversed.reverse_bits()))
}

fn shard_manifest_object_relative(shard_id: ShardId, version: u64) -> String {
    format!("{shard_id}/manifest/{:064b}.binpb", version.reverse_bits())
}

async fn raw_mem_wal_root_inventory(dataset: &Dataset) -> Result<RawMemWalRootInventory, String> {
    let object_store = dataset
        .object_store(None)
        .await
        .map_err(|error| error.to_string())?;
    let root = dataset.branch_location().path.join("_mem_wal");
    let listed = object_store
        .inner
        .list_with_delimiter(Some(&root))
        .await
        .map_err(|error| error.to_string())?;
    let mut shard_ids = Vec::new();
    let mut malformed_prefixes = Vec::new();
    for prefix in listed.common_prefixes {
        let relative = relative_object_path(&root, &prefix)?;
        let component = relative.trim_end_matches('/');
        match ShardId::parse_str(component) {
            Ok(shard_id) if !component.contains('/') => shard_ids.push(shard_id),
            _ => malformed_prefixes.push(relative),
        }
    }
    let mut loose_objects = listed
        .objects
        .iter()
        .map(|metadata| relative_object_path(&root, &metadata.location))
        .collect::<Result<Vec<_>, _>>()?;
    shard_ids.sort();
    malformed_prefixes.sort();
    loose_objects.sort();
    Ok(RawMemWalRootInventory {
        shard_ids,
        malformed_prefixes,
        loose_objects,
    })
}

async fn put_raw_mem_wal_object(dataset: &Dataset, relative: &str) {
    let object_store = dataset.object_store(None).await.unwrap();
    let root = dataset.branch_location().path.join("_mem_wal");
    let path = Path::parse(format!("{}/{relative}", root.as_ref())).unwrap();
    object_store
        .inner
        .put(&path, "gate-e0".into())
        .await
        .unwrap();
}

async fn strict_try_collect<S, T, E>(stream: S) -> Result<Vec<T>, E>
where
    S: TryStream<Ok = T, Error = E> + Unpin,
{
    stream.try_collect().await
}

async fn strict_shard_object_inventory(
    dataset: &Dataset,
    shard_id: ShardId,
) -> Result<StrictShardObjectInventory, String> {
    let object_store = dataset
        .object_store(None)
        .await
        .map_err(|error| error.to_string())?;
    let shard_component = shard_id.as_hyphenated().to_string();
    let shard_path = dataset
        .branch_location()
        .path
        .join("_mem_wal")
        .join(shard_component.as_str());
    // `try_collect` is intentional. Lance's public `list_versions` logs and
    // swallows per-item listing errors; enrollment evidence must instead fail
    // closed if even one recursive listing item cannot be observed.
    let listed = strict_try_collect(object_store.inner.list(Some(&shard_path)))
        .await
        .map_err(|error| error.to_string())?;
    let mut manifest_versions = Vec::new();
    let mut unexpected_objects = Vec::new();
    for metadata in listed {
        let relative = relative_object_path(&shard_path, &metadata.location)?;
        match documented_manifest_version(&relative) {
            Some(Some(version)) => manifest_versions.push(version),
            Some(None) => {}
            None => unexpected_objects.push(relative),
        }
    }
    manifest_versions.sort_unstable();
    unexpected_objects.sort();
    Ok(StrictShardObjectInventory {
        manifest_versions,
        unexpected_objects,
    })
}

async fn unexpected_shard_objects(
    dataset: &Dataset,
    shard_id: ShardId,
) -> Result<Vec<String>, String> {
    Ok(strict_shard_object_inventory(dataset, shard_id)
        .await?
        .unexpected_objects)
}

async fn exact_empty_shard_manifest(
    dataset: &Dataset,
    shard_id: ShardId,
) -> Result<lance_index::mem_wal::ShardManifest, String> {
    let object_store = dataset
        .object_store(None)
        .await
        .map_err(|error| error.to_string())?;
    let branch_path = dataset.branch_location().path;
    let store = ShardManifestStore::new(object_store, &branch_path, shard_id, 2);
    let inventory = strict_shard_object_inventory(dataset, shard_id).await?;
    reject_if(
        inventory.manifest_versions != vec![1],
        format!(
            "expected empty shard has non-initial manifest history: {:?}",
            inventory.manifest_versions
        ),
    )?;
    let manifest = store
        .read_latest()
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "expected shard has no manifest".to_string())?;
    validate_exact_fresh_shard_manifest(&manifest, shard_id)?;
    reject_if(
        !inventory.unexpected_objects.is_empty(),
        format!(
            "expected empty shard has WAL/generation/unknown manifest objects: {:?}",
            inventory.unexpected_objects
        ),
    )?;
    Ok(manifest)
}

fn validate_exact_fresh_shard_manifest(
    manifest: &lance_index::mem_wal::ShardManifest,
    shard_id: ShardId,
) -> Result<(), String> {
    reject_if(
        manifest.shard_id != shard_id
            || manifest.version != 1
            || manifest.shard_spec_id != 1
            || !manifest.shard_field_values.is_empty()
            || manifest.writer_epoch != 1
            || manifest.replay_after_wal_entry_position != 0
            || manifest.wal_entry_position_last_seen != 0
            || manifest.current_generation != 1
            || !manifest.flushed_generations.is_empty()
            || manifest.status != ShardStatus::Active,
        format!("expected shard is not the exact fresh epoch-1 state: {manifest:?}"),
    )
}

async fn assert_high_level_writer_is_exact_empty(
    dataset: &Dataset,
    writer: &ShardWriter,
    expected_shard_id: ShardId,
) {
    assert_eq!(writer.shard_id(), expected_shard_id);
    assert_eq!(writer.epoch(), 1);
    writer.check_fenced().await.unwrap();
    assert_eq!(
        dataset.list_mem_wal_latest_shard_ids().await.unwrap(),
        vec![expected_shard_id],
        "high-level writer open must create exactly the pre-minted shard"
    );
    assert_eq!(
        raw_mem_wal_root_inventory(dataset).await.unwrap(),
        RawMemWalRootInventory {
            shard_ids: vec![expected_shard_id],
            malformed_prefixes: Vec::new(),
            loose_objects: Vec::new(),
        },
        "raw MemWAL root inventory must agree with the high-level singleton shard"
    );

    let memtable = writer.memtable_stats().await.unwrap();
    assert_eq!(memtable.row_count, 0);
    assert_eq!(memtable.batch_count, 0);
    assert_eq!(memtable.generation, 1);
    assert_eq!(memtable.max_buffered_batch_position, None);
    assert_eq!(memtable.max_flushed_batch_position, None);
    assert_eq!(memtable.pending_wal_start_batch_position, None);
    assert_eq!(memtable.pending_wal_end_batch_position, None);
    assert_eq!(memtable.pending_wal_batch_count, 0);
    assert_eq!(memtable.pending_wal_row_count, 0);
    assert_eq!(memtable.pending_wal_estimated_bytes, 0);
    assert_eq!(
        writer.wal_stats().next_wal_entry_position,
        1,
        "a fresh writer's next cursor is one because no 1-based WAL entry exists"
    );

    let stats = writer.stats();
    assert_eq!(stats.put_count, 0);
    assert_eq!(stats.wal_flush_count, 0);
    assert_eq!(stats.wal_flush_bytes, 0);
    assert_eq!(stats.wal_io_count, 0);
    assert_eq!(stats.index_update_count, 0);
    assert_eq!(stats.index_update_rows, 0);
    assert_eq!(stats.memtable_flush_count, 0);
    assert_eq!(stats.memtable_flush_rows, 0);

    let writer_manifest = writer.manifest().await.unwrap().unwrap();
    assert_eq!(
        writer_manifest,
        exact_empty_shard_manifest(dataset, expected_shard_id)
            .await
            .unwrap()
    );
}

fn expected_writer_defaults(enrollment_id: &str) -> HashMap<String, String> {
    HashMap::from([
        (ENROLLMENT_ID_KEY.to_string(), enrollment_id.to_string()),
        (CONFIG_VERSION_KEY.to_string(), CONFIG_VERSION.to_string()),
        ("durable_write".to_string(), "true".to_string()),
        (
            "max_wal_buffer_size".to_string(),
            WAL_BUFFER_BYTES.to_string(),
        ),
    ])
}

fn intended_shard_writer_config(shard_id: ShardId) -> ShardWriterConfig {
    // RC.1 persists defaults in the MemWAL index but does not apply them to a
    // caller-provided writer config. Keep the admission effect exact by
    // reconstructing the intended persisted values explicitly.
    ShardWriterConfig::new(shard_id)
        .with_shard_spec_id(1)
        .with_durable_write(true)
        .with_max_wal_buffer_size(WAL_BUFFER_BYTES)
}

async fn shard_manifest_store(dataset: &Dataset, shard_id: ShardId) -> ShardManifestStore {
    let object_store = dataset.object_store(None).await.unwrap();
    ShardManifestStore::new(object_store, &dataset.branch_location().path, shard_id, 2)
}

async fn capture_pre_enrollment_head(dataset: &Dataset) -> Result<PreEnrollmentHead, String> {
    reject_if(
        dataset
            .mem_wal_index_details()
            .await
            .map_err(|error| error.to_string())?
            .is_some(),
        "pre-enrollment HEAD already contains a MemWAL index",
    )?;

    let transaction = dataset
        .read_transaction()
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "pre-enrollment HEAD has no transaction".to_string())?;
    reject_if(
        transaction.uuid.is_empty(),
        "pre-enrollment transaction UUID is empty",
    )?;

    Ok(PreEnrollmentHead {
        version: dataset.version().version,
        branch_identifier: dataset
            .branch_identifier()
            .await
            .map_err(|error| error.to_string())?,
        transaction_uuid: transaction.uuid,
        manifest_e_tag: dataset.manifest_location().e_tag.clone(),
    })
}

/// Observe the only version chain admitted by the bounded profile.
///
/// The caller holds the exclusive base-HEAD gate and unresolved recovery keeps
/// cleanup out, so attached versions cannot be skipped. Under that boundary,
/// opening deterministic `N` and probing its immediate successor is both
/// history-flat and stricter than resolving "latest". If `N + 1` exists, the
/// same immediate-successor probe on it rejects a buried `N + 2`; otherwise it
/// is the only possible effect. Every probe/open error fails closed.
async fn exact_enrollment_successor_dataset(
    before: &PreEnrollmentHead,
    dataset: &Dataset,
) -> Result<Option<Dataset>, String> {
    let successor_version = before
        .version
        .checked_add(1)
        .ok_or_else(|| "pre-enrollment version cannot have a successor".to_string())?;
    reject_if(
        lance_table::format::is_detached_version(successor_version),
        "pre-enrollment version crosses the detached-version boundary",
    )?;
    let predecessor = dataset
        .checkout_version(before.version)
        .await
        .map_err(|error| format!("cannot read captured enrollment predecessor: {error}"))?;
    let predecessor_transaction = predecessor
        .read_transaction()
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "enrollment predecessor has no transaction".to_string())?;
    reject_if(
        predecessor
            .branch_identifier()
            .await
            .map_err(|error| error.to_string())?
            != before.branch_identifier
            || predecessor_transaction.uuid != before.transaction_uuid
            || predecessor.manifest_location().e_tag != before.manifest_e_tag
            || predecessor
                .mem_wal_index_details()
                .await
                .map_err(|error| error.to_string())?
                .is_some(),
        "enrollment predecessor is not the captured physical no-MemWAL HEAD",
    )?;

    let has_successor = predecessor
        .has_successor_version()
        .await
        .map_err(|error| format!("cannot probe enrollment successor: {error}"))?;
    if !has_successor {
        return Ok(None);
    }

    let buried_version = successor_version
        .checked_add(1)
        .ok_or_else(|| "enrollment successor cannot have a successor".to_string())?;
    reject_if(
        lance_table::format::is_detached_version(buried_version),
        "enrollment successor crosses the detached-version boundary",
    )?;
    let successor = predecessor
        .checkout_version(successor_version)
        .await
        .map_err(|error| format!("cannot read observed enrollment successor: {error}"))?;
    reject_if(
        successor
            .has_successor_version()
            .await
            .map_err(|error| format!("cannot probe buried enrollment effect: {error}"))?,
        format!("enrollment effect is buried by attached version {buried_version}"),
    )?;
    Ok(Some(successor))
}

/// Classify only the exact singleton MemWAL-index successor of `before`.
///
/// This is deliberately stricter than "MemWAL now exists": a wrong operation,
/// an intervening version, a ref-incarnation change, or any configuration drift
/// fails closed. It never resolves latest; all authority comes from exact
/// known-version probes under the bounded profile's exclusive HEAD gate.
async fn classify_exact_enrollment_successor(
    before: &PreEnrollmentHead,
    dataset: &Dataset,
    expected_defaults: &HashMap<String, String>,
) -> Result<ObservedEnrollmentReceipt, String> {
    let successor = exact_enrollment_successor_dataset(before, dataset)
        .await?
        .ok_or_else(|| "exact enrollment successor is absent".to_string())?;
    classify_exact_enrollment_successor_on_exact_dataset(before, &successor, expected_defaults)
        .await
}

async fn classify_exact_enrollment_successor_on_exact_dataset(
    before: &PreEnrollmentHead,
    dataset: &Dataset,
    expected_defaults: &HashMap<String, String>,
) -> Result<ObservedEnrollmentReceipt, String> {
    let version = dataset.version().version;
    let expected_version = before
        .version
        .checked_add(1)
        .ok_or_else(|| "pre-enrollment version cannot have a successor".to_string())?;
    reject_if(
        version != expected_version,
        format!(
            "enrollment is not the exact one-version successor: before={}, after={version}",
            before.version
        ),
    )?;

    let branch_identifier = dataset
        .branch_identifier()
        .await
        .map_err(|error| error.to_string())?;
    reject_if(
        branch_identifier != before.branch_identifier,
        "branch/ref incarnation changed during enrollment",
    )?;

    let transaction = dataset
        .read_transaction()
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "enrollment successor has no transaction".to_string())?;
    reject_if(
        transaction.read_version != before.version,
        format!(
            "enrollment transaction read version {} instead of {}",
            transaction.read_version, before.version
        ),
    )?;
    reject_if(
        transaction.uuid.is_empty() || transaction.uuid == before.transaction_uuid,
        "enrollment successor did not mint a distinct transaction UUID",
    )?;
    reject_if(
        transaction.tag.is_some() || transaction.transaction_properties.is_some(),
        "enrollment successor carries a tag or transaction_properties that the initializer never emits",
    )?;

    let (new_indices, removed_indices) = match &transaction.operation {
        Operation::CreateIndex {
            new_indices,
            removed_indices,
        } => (new_indices, removed_indices),
        _ => return Err("exact enrollment successor is not Operation::CreateIndex".to_string()),
    };
    reject_if(
        new_indices.len() != 1 || !removed_indices.is_empty(),
        "enrollment CreateIndex is not one add with zero removals",
    )?;
    let index = &new_indices[0];
    reject_if(
        index.name != MEM_WAL_INDEX_NAME,
        format!(
            "enrollment created index {:?}, expected {MEM_WAL_INDEX_NAME:?}",
            index.name
        ),
    )?;
    reject_if(
        index.dataset_version != before.version
            || !index.fields.is_empty()
            || index.fragment_bitmap.is_some()
            || index.index_details.is_none()
            || index.index_version != 0
            || index.created_at.is_none()
            || index.base_id.is_some()
            || index.files.is_some(),
        "MemWAL index metadata does not have the exact inline system-index shape",
    )?;

    let details = dataset
        .mem_wal_index_details()
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "CreateIndex successor has no readable MemWAL details".to_string())?;
    reject_if(
        details.snapshot_ts_millis != 0
            || details.num_shards != 1
            || details.inline_snapshots.is_some()
            || details.sharding_specs.len() != 1
            || !details.maintained_indexes.is_empty()
            || !details.merged_generations.is_empty()
            || !details.index_catchup.is_empty(),
        "MemWAL details do not match a fresh unsharded enrollment",
    )?;
    let sharding_spec = &details.sharding_specs[0];
    reject_if(
        sharding_spec.spec_id != 1 || sharding_spec.fields.len() != 1,
        "fresh unsharded enrollment has an unexpected sharding spec",
    )?;
    let sharding_field = &sharding_spec.fields[0];
    reject_if(
        sharding_field.field_id != "bucket"
            || !sharding_field.source_ids.is_empty()
            || sharding_field.transform.as_deref() != Some("unsharded")
            || sharding_field.expression.is_some()
            || sharding_field.result_type != "int32"
            || !sharding_field.parameters.is_empty(),
        "fresh unsharded enrollment has an unexpected sharding field",
    )?;
    reject_if(
        &details.writer_config_defaults != expected_defaults,
        format!(
            "persisted enrollment marker/config mismatch: expected={expected_defaults:?}, actual={:?}",
            details.writer_config_defaults
        ),
    )?;

    let manifest_e_tag = dataset.manifest_location().e_tag.clone();
    if let Some(before_e_tag) = before.manifest_e_tag.as_deref() {
        let after_e_tag = manifest_e_tag
            .as_deref()
            .ok_or_else(|| "enrollment successor lost the manifest e_tag".to_string())?;
        reject_if(
            after_e_tag == before_e_tag,
            "enrollment successor reused the pre-enrollment manifest e_tag",
        )?;
    }

    Ok(ObservedEnrollmentReceipt {
        version,
        transaction_uuid: transaction.uuid.clone(),
        mem_wal_index_uuid: index.uuid,
        manifest_e_tag,
    })
}

async fn classify_combined_enrollment_state(
    before: &PreEnrollmentHead,
    dataset: &Dataset,
    expected_defaults: &HashMap<String, String>,
    expected_shard_id: ShardId,
) -> Result<CombinedEnrollmentState, String> {
    let raw_inventory = raw_mem_wal_root_inventory(dataset).await?;
    let successor = exact_enrollment_successor_dataset(before, dataset).await?;

    if successor.is_none() {
        reject_if(
            !raw_inventory.is_empty(),
            "exact captured no-effect state has MemWAL artifacts",
        )?;
        return Ok(CombinedEnrollmentState::ExactNoEffect);
    }

    let receipt = classify_exact_enrollment_successor_on_exact_dataset(
        before,
        &successor.expect("checked above"),
        expected_defaults,
    )
    .await?;
    if raw_inventory.is_empty() {
        return Ok(CombinedEnrollmentState::ExactIndexOnly(receipt));
    }
    let expected_inventory = RawMemWalRootInventory {
        shard_ids: vec![expected_shard_id],
        malformed_prefixes: Vec::new(),
        loose_objects: Vec::new(),
    };
    reject_if(
        raw_inventory != expected_inventory,
        format!(
            "enrollment has foreign, malformed, extra, or loose artifacts: expected={expected_inventory:?}, actual={raw_inventory:?}"
        ),
    )?;
    exact_empty_shard_manifest(dataset, expected_shard_id).await?;
    Ok(CombinedEnrollmentState::ExactIndexAndExpectedEmptyShard(
        receipt,
    ))
}

async fn current_mem_wal_index_uuid(dataset: &Dataset) -> ShardId {
    let matches = dataset
        .load_indices()
        .await
        .unwrap()
        .iter()
        .filter(|index| index.name == MEM_WAL_INDEX_NAME)
        .map(|index| index.uuid)
        .collect::<Vec<_>>();
    assert_eq!(
        matches.len(),
        1,
        "a valid enrollment must have one singleton MemWAL system index"
    );
    matches[0]
}

async fn initialize_with_defaults(
    dataset: &mut Dataset,
    defaults: &HashMap<String, String>,
) -> lance::Result<()> {
    let mut builder = dataset.initialize_mem_wal().unsharded();
    for (key, value) in defaults {
        builder = builder.add_writer_config_default(key.clone(), value.clone());
    }
    builder.execute().await
}

async fn initialize_bucket_with_defaults(
    dataset: &mut Dataset,
    defaults: &HashMap<String, String>,
) -> lance::Result<()> {
    let mut builder = dataset.initialize_mem_wal().bucket_sharding("id", 1);
    for (key, value) in defaults {
        builder = builder.add_writer_config_default(key.clone(), value.clone());
    }
    builder.execute().await
}

async fn initialize_and_close_empty_shard(
    dataset: &mut Dataset,
    defaults: &HashMap<String, String>,
    shard_id: ShardId,
) {
    initialize_with_defaults(dataset, defaults).await.unwrap();
    let writer = dataset
        .mem_wal_writer(shard_id, intended_shard_writer_config(shard_id))
        .await
        .unwrap();
    writer.close().await.unwrap();
}

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

fn one_row(dataset: &Dataset, id: &str, value: i32) -> RecordBatch {
    let schema = Arc::new(Schema::from(dataset.schema()));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![id])),
            Arc::new(Int32Array::from(vec![value])),
        ],
    )
    .unwrap()
}

async fn append_row(dataset: &mut Dataset, id: &str, value: i32) {
    let batch = one_row(dataset, id, value);
    let reader = RecordBatchIterator::new([Ok(batch.clone())], batch.schema());
    dataset
        .append(
            reader,
            Some(WriteParams {
                mode: WriteMode::Append,
                enable_stable_row_ids: true,
                data_storage_version: Some(LanceFileVersion::V2_2),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
}

async fn merge_row_and_mark_generation(
    dataset: Dataset,
    id: &str,
    value: i32,
    merged_generation: MergedGeneration,
) -> Dataset {
    let batch = one_row(&dataset, id, value);
    let reader = RecordBatchIterator::new([Ok(batch.clone())], batch.schema());
    let mut builder =
        MergeInsertBuilder::try_new(Arc::new(dataset), vec!["id".to_string()]).unwrap();
    builder
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .conflict_retries(0)
        .mark_generations_as_merged(vec![merged_generation]);
    let (dataset, stats) = builder
        .try_build()
        .unwrap()
        .execute_reader(Box::new(reader))
        .await
        .unwrap();
    assert_eq!(stats.num_inserted_rows, 1);
    assert_eq!(stats.num_updated_rows, 0);
    (*dataset).clone()
}

fn witness_object_kind(path: &str) -> &'static str {
    if path.contains("/_versions/") || path.ends_with(".manifest") {
        "manifest"
    } else if path.contains("/_transactions/") {
        "transaction"
    } else if path.contains("/_indices/") {
        "index"
    } else {
        "other"
    }
}

async fn prepare_enrollment_at_baseline_version(
    uri: &str,
    baseline_version: u64,
    enrollment_id: &str,
) -> (PreEnrollmentHead, HashMap<String, String>) {
    let mut dataset = fresh_dataset(uri).await;
    while dataset.version().version < baseline_version {
        let version = dataset.version().version + 1;
        let transaction = Transaction::new(
            dataset.version().version,
            Operation::UpdateConfig {
                config_updates: Some(UpdateMap {
                    update_entries: vec![
                        (
                            "omnigraph.gate_e0_history".to_string(),
                            Some(format!("{version:020}")),
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
        dataset = CommitBuilder::new(Arc::new(dataset))
            .execute(transaction)
            .await
            .unwrap();
    }
    assert_eq!(dataset.version().version, baseline_version);
    let before = capture_pre_enrollment_head(&dataset).await.unwrap();
    let defaults = expected_writer_defaults(enrollment_id);
    initialize_with_defaults(&mut dataset, &defaults)
        .await
        .unwrap();
    assert_eq!(dataset.version().version, baseline_version + 1);
    (before, defaults)
}

/// Capture a witness from a Dataset already opened at the exact version named
/// by durable authority. This deliberately does not resolve latest. Phase A
/// must hold the exclusive base-HEAD gate across selecting that version and
/// consuming the witness; rereading a pinned Dataset is not a race detector.
async fn capture_witness_from_exact_dataset(
    dataset: &Dataset,
) -> Result<CurrentHeadWitness, String> {
    let branch_before = dataset
        .branch_identifier()
        .await
        .map_err(|error| error.to_string())?;
    let version = dataset.version().version;
    let transaction = dataset
        .read_transaction()
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "current HEAD has no transaction".to_string())?;
    reject_if(
        transaction.uuid.is_empty(),
        "current HEAD has an empty transaction UUID",
    )?;
    let manifest_e_tag = dataset.manifest_location().e_tag.clone();
    let branch_after = dataset
        .branch_identifier()
        .await
        .map_err(|error| error.to_string())?;
    reject_if(
        branch_before != branch_after || dataset.version().version != version,
        "exact-version witness is internally incoherent",
    )?;
    Ok(CurrentHeadWitness {
        version,
        branch_identifier: branch_before,
        transaction_uuid: transaction.uuid,
        manifest_e_tag,
    })
}

async fn measure_exact_enrollment_probe(
    uri: &str,
    before: &PreEnrollmentHead,
    expected_defaults: &HashMap<String, String>,
) -> ExactEnrollmentProbeCost {
    let tracker = AttemptTracker::default();
    let anchor = open_attempt_tracked_lance_dataset_at_version(
        uri,
        before.version,
        Arc::new(Session::default()),
        &tracker,
    )
    .await
    .unwrap();
    let _anchor_open_attempts = tracker.incremental_attempts();
    let receipt = classify_exact_enrollment_successor(before, &anchor, expected_defaults)
        .await
        .unwrap();
    let successor = anchor.checkout_version(before.version + 1).await.unwrap();
    let witness = capture_witness_from_exact_dataset(&successor)
        .await
        .unwrap();
    let attempts = tracker.incremental_attempts();
    assert_eq!(receipt.version, before.version + 1);
    assert_eq!(witness.version, receipt.version);
    assert_eq!(
        witness.branch_identifier,
        successor.branch_identifier().await.unwrap()
    );
    assert_eq!(
        witness.manifest_e_tag, receipt.manifest_e_tag,
        "witness must carry the current manifest e_tag"
    );
    let transaction = successor
        .read_transaction()
        .await
        .unwrap()
        .expect("current-head witness requires the current transaction");
    assert_eq!(witness.transaction_uuid, transaction.uuid);
    assert!(matches!(
        transaction.operation,
        Operation::CreateIndex { .. }
    ));
    assert!(
        !attempts.is_empty(),
        "tracked exact-version classifier must issue reads"
    );
    assert!(
        attempts.iter().all(|attempt| {
            attempt.outcome != AttemptOutcome::Pending && !attempt.method.starts_with("list")
        }),
        "known-version enrollment classification must complete every attempt without resolving latest/listing history: {attempts:?}"
    );
    let mut attempt_shape = attempts
        .iter()
        .map(|attempt| ProbeAttemptShape {
            method: attempt.method,
            object_kind: witness_object_kind(attempt.path.as_ref()),
            outcome: attempt.outcome,
        })
        .collect::<Vec<_>>();
    attempt_shape.sort();
    ExactEnrollmentProbeCost {
        attempts: attempt_shape,
    }
}

#[tokio::test]
async fn exact_enrollment_marker_survives_reopen_ordinary_commit_and_merge_progress() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("marker-survival.lance");
    let uri = uri.to_str().unwrap();
    let mut dataset = fresh_dataset(uri).await;
    let before = capture_pre_enrollment_head(&dataset).await.unwrap();
    let enrollment_id = ShardId::new_v4().to_string();
    let expected_defaults = expected_writer_defaults(&enrollment_id);

    initialize_with_defaults(&mut dataset, &expected_defaults)
        .await
        .unwrap();
    drop(dataset);

    // Reopen instead of trusting the mutating initializer handle. This is the
    // local lost-ack shape: only the precondition and durable Lance state remain.
    let mut reopened = DatasetBuilder::from_uri(uri).load().await.unwrap();
    let enrollment_receipt =
        classify_exact_enrollment_successor(&before, &reopened, &expected_defaults)
            .await
            .unwrap();
    let reopened_again = DatasetBuilder::from_uri(uri).load().await.unwrap();
    assert_eq!(
        enrollment_receipt,
        classify_exact_enrollment_successor(&before, &reopened_again, &expected_defaults)
            .await
            .unwrap(),
        "the observed enrollment receipt must be stable across unchanged reopens"
    );

    append_row(&mut reopened, "bob", 2).await;
    let after_append = reopened.mem_wal_index_details().await.unwrap().unwrap();
    assert_eq!(after_append.writer_config_defaults, expected_defaults);
    assert!(after_append.merged_generations.is_empty());
    assert_eq!(
        current_mem_wal_index_uuid(&reopened).await,
        enrollment_receipt.mem_wal_index_uuid,
        "ordinary data commits must preserve the MemWAL index UUID"
    );
    assert!(
        classify_exact_enrollment_successor(&before, &reopened, &expected_defaults)
            .await
            .is_err(),
        "the enrollment classifier must stay one-successor-only after an ordinary commit"
    );

    let shard_id = ShardId::new_v4();
    let merged_generation = MergedGeneration::new(shard_id, 7);
    let reopened =
        merge_row_and_mark_generation(reopened, "carol", 3, merged_generation.clone()).await;
    let after_merge = reopened.mem_wal_index_details().await.unwrap().unwrap();
    assert_eq!(after_merge.writer_config_defaults, expected_defaults);
    assert_eq!(after_merge.merged_generations, vec![merged_generation]);
    assert_ne!(
        current_mem_wal_index_uuid(&reopened).await,
        enrollment_receipt.mem_wal_index_uuid,
        "merged-generation updates replace MemWAL index metadata, so enrollment identity cannot be its UUID"
    );

    let reopened_after_merge = DatasetBuilder::from_uri(uri).load().await.unwrap();
    let persisted = reopened_after_merge
        .mem_wal_index_details()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(persisted.writer_config_defaults, expected_defaults);
    assert_eq!(
        persisted
            .merged_generations
            .iter()
            .find(|generation| generation.shard_id == shard_id)
            .map(|generation| generation.generation),
        Some(7)
    );
}

#[tokio::test]
async fn exact_enrollment_classifier_fails_closed_on_wrong_and_intervening_states() {
    let dir = tempfile::tempdir().unwrap();

    let foreign_uri = dir.path().join("foreign-marker.lance");
    let foreign_uri = foreign_uri.to_str().unwrap();
    let mut foreign = fresh_dataset(foreign_uri).await;
    let foreign_before = capture_pre_enrollment_head(&foreign).await.unwrap();
    let actual_defaults = expected_writer_defaults("foreign-enrollment");
    initialize_with_defaults(&mut foreign, &actual_defaults)
        .await
        .unwrap();
    let expected_defaults = expected_writer_defaults("our-enrollment");
    let wrong_marker =
        classify_exact_enrollment_successor(&foreign_before, &foreign, &expected_defaults)
            .await
            .unwrap_err();
    assert!(wrong_marker.contains("marker/config mismatch"));

    let wrong_operation_uri = dir.path().join("wrong-operation.lance");
    let wrong_operation_uri = wrong_operation_uri.to_str().unwrap();
    let mut wrong_operation = fresh_dataset(wrong_operation_uri).await;
    let wrong_operation_before = capture_pre_enrollment_head(&wrong_operation).await.unwrap();
    append_row(&mut wrong_operation, "bob", 2).await;
    let wrong_operation_error = classify_exact_enrollment_successor(
        &wrong_operation_before,
        &wrong_operation,
        &expected_defaults,
    )
    .await
    .unwrap_err();
    assert!(wrong_operation_error.contains("Operation::CreateIndex"));

    let foreign_index_uri = dir.path().join("foreign-real-index.lance");
    let foreign_index_uri = foreign_index_uri.to_str().unwrap();
    let mut foreign_index = fresh_dataset(foreign_index_uri).await;
    let foreign_index_before = capture_pre_enrollment_head(&foreign_index).await.unwrap();
    foreign_index
        .create_index_builder(&["value"], IndexType::BTree, &ScalarIndexParams::default())
        .await
        .unwrap();
    let foreign_index_transaction = foreign_index.read_transaction().await.unwrap().unwrap();
    assert!(matches!(
        foreign_index_transaction.operation,
        Operation::CreateIndex { .. }
    ));
    assert_eq!(
        foreign_index.version().version,
        foreign_index_before.version + 1
    );
    let foreign_index_error = classify_exact_enrollment_successor(
        &foreign_index_before,
        &foreign_index,
        &expected_defaults,
    )
    .await
    .unwrap_err();
    assert!(foreign_index_error.contains("expected \"__lance_mem_wal\""));

    let intervening_uri = dir.path().join("intervening-version.lance");
    let intervening_uri = intervening_uri.to_str().unwrap();
    let mut intervening = fresh_dataset(intervening_uri).await;
    let intervening_before = capture_pre_enrollment_head(&intervening).await.unwrap();
    append_row(&mut intervening, "bob", 2).await;
    initialize_with_defaults(&mut intervening, &expected_defaults)
        .await
        .unwrap();
    let intervening_error =
        classify_exact_enrollment_successor(&intervening_before, &intervening, &expected_defaults)
            .await
            .unwrap_err();
    assert!(intervening_error.contains("enrollment effect is buried"));
}

#[tokio::test]
async fn exact_enrollment_classifier_rejects_wrong_sharding_tag_and_properties() {
    let dir = tempfile::tempdir().unwrap();
    let defaults = expected_writer_defaults("exact-negative-operation-shape");

    let wrong_sharding_uri = dir.path().join("wrong-sharding.lance");
    let wrong_sharding_uri = wrong_sharding_uri.to_str().unwrap();
    let mut wrong_sharding = fresh_dataset(wrong_sharding_uri).await;
    let wrong_sharding_before = capture_pre_enrollment_head(&wrong_sharding).await.unwrap();
    initialize_bucket_with_defaults(&mut wrong_sharding, &defaults)
        .await
        .unwrap();
    let wrong_sharding_error =
        classify_exact_enrollment_successor(&wrong_sharding_before, &wrong_sharding, &defaults)
            .await
            .unwrap_err();
    assert!(wrong_sharding_error.contains("fresh unsharded enrollment"));

    // Reuse the initializer's exact public CreateIndex operation so tag and
    // transaction-properties are the only differences in the two successors.
    let template_uri = dir.path().join("operation-template.lance");
    let template_uri = template_uri.to_str().unwrap();
    let mut template = fresh_dataset(template_uri).await;
    initialize_with_defaults(&mut template, &defaults)
        .await
        .unwrap();
    let enrollment_operation = template
        .read_transaction()
        .await
        .unwrap()
        .unwrap()
        .operation
        .clone();
    assert!(matches!(
        enrollment_operation,
        Operation::CreateIndex { .. }
    ));

    let tagged_uri = dir.path().join("tagged-successor.lance");
    let tagged_uri = tagged_uri.to_str().unwrap();
    let tagged = fresh_dataset(tagged_uri).await;
    let tagged_before = capture_pre_enrollment_head(&tagged).await.unwrap();
    let tagged_transaction =
        TransactionBuilder::new(tagged_before.version, enrollment_operation.clone())
            .tag(Some("foreign-tag".to_string()))
            .build();
    let tagged = CommitBuilder::new(Arc::new(tagged))
        .execute(tagged_transaction)
        .await
        .unwrap();
    let tagged_error = classify_exact_enrollment_successor(&tagged_before, &tagged, &defaults)
        .await
        .unwrap_err();
    assert!(tagged_error.contains("tag or transaction_properties"));

    let properties_uri = dir.path().join("properties-successor.lance");
    let properties_uri = properties_uri.to_str().unwrap();
    let properties = fresh_dataset(properties_uri).await;
    let properties_before = capture_pre_enrollment_head(&properties).await.unwrap();
    let properties_transaction =
        TransactionBuilder::new(properties_before.version, enrollment_operation)
            .transaction_properties(Some(Arc::new(HashMap::from([(
                "foreign-key".to_string(),
                "foreign-value".to_string(),
            )]))))
            .build();
    let properties = CommitBuilder::new(Arc::new(properties))
        .execute(properties_transaction)
        .await
        .unwrap();
    let properties_error =
        classify_exact_enrollment_successor(&properties_before, &properties, &defaults)
            .await
            .unwrap_err();
    assert!(properties_error.contains("tag or transaction_properties"));
}

#[tokio::test]
async fn combined_classifier_rejects_raw_memwal_artifacts_in_every_state() {
    let dir = tempfile::tempdir().unwrap();

    let foreign_uri = dir.path().join("no-effect-foreign-shard.lance");
    let foreign_uri = foreign_uri.to_str().unwrap();
    let foreign = fresh_dataset(foreign_uri).await;
    let foreign_before = capture_pre_enrollment_head(&foreign).await.unwrap();
    let foreign_defaults = expected_writer_defaults("no-effect-foreign-shard");
    let foreign_shard_id = ShardId::new_v4();
    shard_manifest_store(&foreign, foreign_shard_id)
        .await
        .initialize_shard(1, HashMap::new())
        .await
        .unwrap();
    let foreign_error = classify_combined_enrollment_state(
        &foreign_before,
        &foreign,
        &foreign_defaults,
        ShardId::new_v4(),
    )
    .await
    .unwrap_err();
    assert!(foreign_error.contains("exact captured no-effect state"));

    let malformed_uri = dir.path().join("no-effect-malformed-prefix.lance");
    let malformed_uri = malformed_uri.to_str().unwrap();
    let malformed = fresh_dataset(malformed_uri).await;
    let malformed_before = capture_pre_enrollment_head(&malformed).await.unwrap();
    let malformed_defaults = expected_writer_defaults("no-effect-malformed-prefix");
    put_raw_mem_wal_object(&malformed, "not-a-uuid/manifest/rogue.tmp").await;
    let malformed_error = classify_combined_enrollment_state(
        &malformed_before,
        &malformed,
        &malformed_defaults,
        ShardId::new_v4(),
    )
    .await
    .unwrap_err();
    assert!(malformed_error.contains("exact captured no-effect state"));

    let loose_uri = dir.path().join("no-effect-loose-object.lance");
    let loose_uri = loose_uri.to_str().unwrap();
    let loose = fresh_dataset(loose_uri).await;
    let loose_before = capture_pre_enrollment_head(&loose).await.unwrap();
    let loose_defaults = expected_writer_defaults("no-effect-loose-object");
    put_raw_mem_wal_object(&loose, "loose-object").await;
    let loose_error = classify_combined_enrollment_state(
        &loose_before,
        &loose,
        &loose_defaults,
        ShardId::new_v4(),
    )
    .await
    .unwrap_err();
    assert!(loose_error.contains("exact captured no-effect state"));

    let unexpected_uri = dir.path().join("unexpected-manifest-key.lance");
    let unexpected_uri = unexpected_uri.to_str().unwrap();
    let mut unexpected = fresh_dataset(unexpected_uri).await;
    let unexpected_before = capture_pre_enrollment_head(&unexpected).await.unwrap();
    let unexpected_defaults = expected_writer_defaults("unexpected-manifest-key");
    initialize_with_defaults(&mut unexpected, &unexpected_defaults)
        .await
        .unwrap();
    let expected_shard_id = ShardId::new_v4();
    let writer = unexpected
        .mem_wal_writer(
            expected_shard_id,
            intended_shard_writer_config(expected_shard_id),
        )
        .await
        .unwrap();
    writer.close().await.unwrap();
    put_raw_mem_wal_object(
        &unexpected,
        &format!("{expected_shard_id}/manifest/rogue.tmp"),
    )
    .await;
    let unexpected_error = classify_combined_enrollment_state(
        &unexpected_before,
        &unexpected,
        &unexpected_defaults,
        expected_shard_id,
    )
    .await
    .unwrap_err();
    assert!(
        unexpected_error.contains("unknown manifest objects"),
        "unexpected manifest key must fail through the exact filename whitelist: {unexpected_error}"
    );
}

#[tokio::test]
async fn strict_inventory_collection_rejects_a_partial_listing() {
    let listing = futures::stream::iter(vec![
        Ok::<_, &'static str>("visible-object"),
        Err("injected-list-page-failure"),
    ]);
    assert_eq!(
        strict_try_collect(listing).await.unwrap_err(),
        "injected-list-page-failure",
        "a successful prefix followed by a listing error must never become partial clean evidence"
    );
}

#[test]
fn exact_enrollment_chain_cannot_regress_to_latest_or_history_listing() {
    let source = include_str!("memwal_enrollment_gate.rs");
    let start = source
        .find("async fn exact_enrollment_successor_dataset")
        .expect("exact enrollment-chain classifier must exist");
    let end = source[start..]
        .find("/// Classify only the exact singleton")
        .map(|offset| start + offset)
        .expect("exact enrollment-chain classifier boundary must exist");
    let classifier = &source[start..end];
    for forbidden in [
        ".checkout_latest(",
        ".latest_version_id(",
        ".is_stale(",
        ".versions(",
    ] {
        assert!(
            !classifier.contains(forbidden),
            "Gate E0's bounded classifier must use exact immediate-successor probes, not {forbidden}"
        );
    }
    assert!(classifier.contains(".has_successor_version()"));
    assert!(classifier.contains(".checkout_version(before.version)"));
    assert!(classifier.contains(".checkout_version(successor_version)"));
}

#[tokio::test]
async fn empty_shard_classifier_rejects_cursor_and_flushed_generation_progress() {
    let shard_id = ShardId::new_v4();
    let exact = lance_index::mem_wal::ShardManifest {
        shard_id,
        version: 1,
        shard_spec_id: 1,
        shard_field_values: HashMap::new(),
        writer_epoch: 1,
        replay_after_wal_entry_position: 0,
        wal_entry_position_last_seen: 0,
        current_generation: 1,
        flushed_generations: Vec::new(),
        status: ShardStatus::Active,
    };
    validate_exact_fresh_shard_manifest(&exact, shard_id).unwrap();

    let cursor = lance_index::mem_wal::ShardManifest {
        wal_entry_position_last_seen: 1,
        ..exact.clone()
    };
    assert!(validate_exact_fresh_shard_manifest(&cursor, shard_id).is_err());

    let replay_cursor = lance_index::mem_wal::ShardManifest {
        replay_after_wal_entry_position: 1,
        ..exact.clone()
    };
    assert!(validate_exact_fresh_shard_manifest(&replay_cursor, shard_id).is_err());

    let flushed = lance_index::mem_wal::ShardManifest {
        flushed_generations: vec![FlushedGeneration {
            generation: 1,
            path: "orphan-generation-1".to_string(),
        }],
        ..exact
    };
    assert!(validate_exact_fresh_shard_manifest(&flushed, shard_id).is_err());
}

#[tokio::test]
async fn combined_classifier_rejects_persisted_cursor_and_flushed_generation_progress() {
    let dir = tempfile::tempdir().unwrap();

    let cursor_uri = dir.path().join("persisted-cursor-progress.lance");
    let cursor_uri = cursor_uri.to_str().unwrap();
    let mut cursor_dataset = fresh_dataset(cursor_uri).await;
    let cursor_before = capture_pre_enrollment_head(&cursor_dataset).await.unwrap();
    let cursor_defaults = expected_writer_defaults("persisted-cursor-progress");
    let cursor_shard_id = ShardId::new_v4();
    initialize_and_close_empty_shard(&mut cursor_dataset, &cursor_defaults, cursor_shard_id).await;
    let cursor_store = shard_manifest_store(&cursor_dataset, cursor_shard_id).await;
    cursor_store
        .commit_update(1, |current| lance_index::mem_wal::ShardManifest {
            version: current.version + 1,
            wal_entry_position_last_seen: 1,
            ..current.clone()
        })
        .await
        .unwrap();
    let persisted_cursor = cursor_store.read_latest().await.unwrap().unwrap();
    assert_eq!(persisted_cursor.version, 2);
    assert_eq!(persisted_cursor.wal_entry_position_last_seen, 1);
    assert_eq!(
        strict_shard_object_inventory(&cursor_dataset, cursor_shard_id)
            .await
            .unwrap()
            .manifest_versions,
        vec![1, 2]
    );
    let cursor_error = classify_combined_enrollment_state(
        &cursor_before,
        &cursor_dataset,
        &cursor_defaults,
        cursor_shard_id,
    )
    .await
    .unwrap_err();
    assert!(cursor_error.contains("non-initial manifest history"));

    let flushed_uri = dir.path().join("persisted-flushed-generation.lance");
    let flushed_uri = flushed_uri.to_str().unwrap();
    let mut flushed_dataset = fresh_dataset(flushed_uri).await;
    let flushed_before = capture_pre_enrollment_head(&flushed_dataset).await.unwrap();
    let flushed_defaults = expected_writer_defaults("persisted-flushed-generation");
    let flushed_shard_id = ShardId::new_v4();
    initialize_and_close_empty_shard(&mut flushed_dataset, &flushed_defaults, flushed_shard_id)
        .await;
    let flushed_store = shard_manifest_store(&flushed_dataset, flushed_shard_id).await;
    flushed_store
        .commit_update(1, |current| lance_index::mem_wal::ShardManifest {
            version: current.version + 1,
            current_generation: 2,
            flushed_generations: vec![FlushedGeneration {
                generation: 1,
                path: "generation-1.lance".to_string(),
            }],
            ..current.clone()
        })
        .await
        .unwrap();
    let persisted_flushed = flushed_store.read_latest().await.unwrap().unwrap();
    assert_eq!(persisted_flushed.version, 2);
    assert_eq!(persisted_flushed.current_generation, 2);
    assert_eq!(persisted_flushed.flushed_generations.len(), 1);
    assert_eq!(
        strict_shard_object_inventory(&flushed_dataset, flushed_shard_id)
            .await
            .unwrap()
            .manifest_versions,
        vec![1, 2]
    );
    let flushed_error = classify_combined_enrollment_state(
        &flushed_before,
        &flushed_dataset,
        &flushed_defaults,
        flushed_shard_id,
    )
    .await
    .unwrap_err();
    assert!(flushed_error.contains("non-initial manifest history"));
}

#[tokio::test]
async fn combined_classifier_rejects_a_corrupt_latest_shard_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("corrupt-latest-shard-manifest.lance");
    let uri = uri.to_str().unwrap();
    let mut dataset = fresh_dataset(uri).await;
    let before = capture_pre_enrollment_head(&dataset).await.unwrap();
    let defaults = expected_writer_defaults("corrupt-latest-shard-manifest");
    let shard_id = ShardId::new_v4();
    initialize_and_close_empty_shard(&mut dataset, &defaults, shard_id).await;
    assert!(matches!(
        classify_combined_enrollment_state(&before, &dataset, &defaults, shard_id)
            .await
            .unwrap(),
        CombinedEnrollmentState::ExactIndexAndExpectedEmptyShard(_)
    ));

    put_raw_mem_wal_object(&dataset, &shard_manifest_object_relative(shard_id, 1)).await;
    let inventory = strict_shard_object_inventory(&dataset, shard_id)
        .await
        .unwrap();
    assert_eq!(inventory.manifest_versions, vec![1]);
    assert!(inventory.unexpected_objects.is_empty());
    let corrupt_error = classify_combined_enrollment_state(&before, &dataset, &defaults, shard_id)
        .await
        .unwrap_err();
    assert!(
        corrupt_error.contains("decode") || corrupt_error.contains("protobuf"),
        "corrupt latest manifest must fail closed after strict enumeration: {corrupt_error}"
    );
}

#[tokio::test]
async fn combined_classifier_uses_exact_successor_probes_from_stale_handles() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("stale-handles.lance");
    let uri = uri.to_str().unwrap();
    let mut authority = fresh_dataset(uri).await;
    let stale_at_n = DatasetBuilder::from_uri(uri).load().await.unwrap();
    let before = capture_pre_enrollment_head(&stale_at_n).await.unwrap();
    let defaults = expected_writer_defaults("stale-handles");
    let expected_shard_id = ShardId::new_v4();

    initialize_with_defaults(&mut authority, &defaults)
        .await
        .unwrap();
    assert!(matches!(
        classify_combined_enrollment_state(&before, &stale_at_n, &defaults, expected_shard_id,)
            .await
            .unwrap(),
        CombinedEnrollmentState::ExactIndexOnly(_)
    ));

    let stale_at_n_plus_one = DatasetBuilder::from_uri(uri).load().await.unwrap();
    assert_eq!(stale_at_n_plus_one.version().version, before.version + 1);
    append_row(&mut authority, "intervening", 2).await;
    let buried_error = classify_combined_enrollment_state(
        &before,
        &stale_at_n_plus_one,
        &defaults,
        expected_shard_id,
    )
    .await
    .unwrap_err();
    assert!(buried_error.contains("enrollment effect is buried"));
}

#[tokio::test]
async fn high_level_writer_and_combined_enrollment_truth_table_are_exact() {
    let dir = tempfile::tempdir().unwrap();

    let positive_uri = dir.path().join("combined-positive.lance");
    let positive_uri = positive_uri.to_str().unwrap();
    let mut positive = fresh_dataset(positive_uri).await;
    let positive_before = capture_pre_enrollment_head(&positive).await.unwrap();
    let positive_defaults = expected_writer_defaults("combined-positive");
    let expected_shard_id = ShardId::new_v4();
    assert_eq!(
        classify_combined_enrollment_state(
            &positive_before,
            &positive,
            &positive_defaults,
            expected_shard_id,
        )
        .await
        .unwrap(),
        CombinedEnrollmentState::ExactNoEffect
    );

    initialize_with_defaults(&mut positive, &positive_defaults)
        .await
        .unwrap();
    let index_only = classify_combined_enrollment_state(
        &positive_before,
        &positive,
        &positive_defaults,
        expected_shard_id,
    )
    .await
    .unwrap();
    assert!(matches!(
        index_only,
        CombinedEnrollmentState::ExactIndexOnly(_)
    ));

    let writer = positive
        .mem_wal_writer(
            expected_shard_id,
            intended_shard_writer_config(expected_shard_id),
        )
        .await
        .unwrap();
    assert_high_level_writer_is_exact_empty(&positive, &writer, expected_shard_id).await;
    let index_and_shard = classify_combined_enrollment_state(
        &positive_before,
        &positive,
        &positive_defaults,
        expected_shard_id,
    )
    .await
    .unwrap();
    assert!(matches!(
        index_and_shard,
        CombinedEnrollmentState::ExactIndexAndExpectedEmptyShard(_)
    ));
    writer.close().await.unwrap();

    let foreign_uri = dir.path().join("combined-foreign-shard.lance");
    let foreign_uri = foreign_uri.to_str().unwrap();
    let mut foreign = fresh_dataset(foreign_uri).await;
    let foreign_before = capture_pre_enrollment_head(&foreign).await.unwrap();
    let foreign_defaults = expected_writer_defaults("combined-foreign");
    initialize_with_defaults(&mut foreign, &foreign_defaults)
        .await
        .unwrap();
    let foreign_shard_id = ShardId::new_v4();
    let expected_foreign_shard_id = ShardId::new_v4();
    let foreign_writer = foreign
        .mem_wal_writer(
            foreign_shard_id,
            intended_shard_writer_config(foreign_shard_id),
        )
        .await
        .unwrap();
    let foreign_error = classify_combined_enrollment_state(
        &foreign_before,
        &foreign,
        &foreign_defaults,
        expected_foreign_shard_id,
    )
    .await
    .unwrap_err();
    assert!(foreign_error.contains("foreign, malformed, extra, or loose artifacts"));
    foreign_writer.close().await.unwrap();

    let data_uri = dir.path().join("combined-data-bearing.lance");
    let data_uri = data_uri.to_str().unwrap();
    let mut data_bearing = fresh_dataset(data_uri).await;
    let data_before = capture_pre_enrollment_head(&data_bearing).await.unwrap();
    let data_defaults = expected_writer_defaults("combined-data-bearing");
    initialize_with_defaults(&mut data_bearing, &data_defaults)
        .await
        .unwrap();
    let data_shard_id = ShardId::new_v4();
    let data_writer = data_bearing
        .mem_wal_writer(data_shard_id, intended_shard_writer_config(data_shard_id))
        .await
        .unwrap();
    let write = data_writer
        .put(vec![one_row(&data_bearing, "wal-row", 99)])
        .await
        .unwrap();
    assert_eq!(write.batch_positions, 0..1);
    assert!(
        !unexpected_shard_objects(&data_bearing, data_shard_id)
            .await
            .unwrap()
            .is_empty(),
        "a durable put must leave an observable WAL object"
    );
    assert!(
        classify_combined_enrollment_state(
            &data_before,
            &data_bearing,
            &data_defaults,
            data_shard_id,
        )
        .await
        .is_err(),
        "a data-bearing shard is never an admissible enrollment-only effect"
    );
    data_writer.abort().await.unwrap();
}

#[tokio::test]
async fn exact_known_version_enrollment_probe_is_flat_across_history_depth() {
    const SHALLOW_VERSION: u64 = 8;
    const DEEP_VERSION: u64 = 80;

    let dir = tempfile::tempdir().unwrap();
    let shallow_uri = dir.path().join("exact-probe-shallow.lance");
    let shallow_uri = shallow_uri.to_str().unwrap();
    let (shallow_before, shallow_defaults) =
        prepare_enrollment_at_baseline_version(shallow_uri, SHALLOW_VERSION, "exact-probe-shallow")
            .await;
    let shallow =
        measure_exact_enrollment_probe(shallow_uri, &shallow_before, &shallow_defaults).await;

    let deep_uri = dir.path().join("exact-probe-deep.lance");
    let deep_uri = deep_uri.to_str().unwrap();
    let (deep_before, deep_defaults) =
        prepare_enrollment_at_baseline_version(deep_uri, DEEP_VERSION, "exact-probe-deep").await;
    let deep = measure_exact_enrollment_probe(deep_uri, &deep_before, &deep_defaults).await;

    assert_eq!(
        shallow.attempts, deep.attempts,
        "exact known-version enrollment/witness attempt shape, including the expected NotFound successor probe, must stay flat as retained history grows; shallow={shallow:?}, deep={deep:?}"
    );
    assert!(shallow.attempts.len() <= 8);
    eprintln!(
        "MemWAL exact enrollment/witness probe: shallow_version={SHALLOW_VERSION} shallow={shallow:?}; \
         deep_version={DEEP_VERSION} deep={deep:?}"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn exact_successor_probe_does_not_enumerate_the_versions_directory() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let dataset_path = dir.path().join("exact-probe-no-list.lance");
    let uri = dataset_path.to_str().unwrap();
    let mut current = fresh_dataset(uri).await;
    for version in 2..=16 {
        append_row(
            &mut current,
            &format!("no-list-history-{version}"),
            version as i32,
        )
        .await;
    }
    let predecessor = current.checkout_version(1).await.unwrap();
    let versions_dir = dataset_path.join("_versions");
    let original_permissions = std::fs::metadata(&versions_dir).unwrap().permissions();
    let mut execute_only = original_permissions.clone();
    execute_only.set_mode(0o111);
    std::fs::set_permissions(&versions_dir, execute_only).unwrap();

    // Capture both results before restoring permissions so a failed assertion
    // cannot strand an unreadable temp directory.
    let successor_probe = predecessor.has_successor_version().await;
    let exact_successor = predecessor.checkout_version(2).await;
    let mut latest_probe = predecessor.clone();
    let latest_resolution = latest_probe.checkout_latest().await;
    std::fs::set_permissions(&versions_dir, original_permissions.clone()).unwrap();

    assert_eq!(successor_probe.unwrap(), true);
    assert_eq!(exact_successor.unwrap().version().version, 2);
    assert!(
        latest_resolution.is_err(),
        "the pinned local latest resolver should be unable to enumerate an execute-only versions directory; this negative control proves the exact successor path did not list history"
    );

    let mut no_access = original_permissions.clone();
    no_access.set_mode(0o000);
    std::fs::set_permissions(&versions_dir, no_access).unwrap();
    let denied_successor_probe = predecessor.has_successor_version().await;
    std::fs::set_permissions(&versions_dir, original_permissions).unwrap();
    assert!(
        denied_successor_probe.is_err(),
        "an unreadable exact successor must be an error, never false/no-effect"
    );
}

#[tokio::test]
async fn public_shard_lifecycle_can_seal_refuse_reopen_and_claim() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("shard-lifecycle.lance");
    let uri = uri.to_str().unwrap();
    let mut dataset = fresh_dataset(uri).await;
    let defaults = expected_writer_defaults("shard-lifecycle-enrollment");
    initialize_with_defaults(&mut dataset, &defaults)
        .await
        .unwrap();

    let shard_id = ShardId::new_v4();
    let object_store = dataset.object_store(None).await.unwrap();
    let branch_path = dataset.branch_location().path;
    let store = ShardManifestStore::new(object_store, &branch_path, shard_id, 2);
    let initial = store.initialize_shard(1, HashMap::new()).await.unwrap();
    assert_eq!(initial.version, 1);
    assert_eq!(initial.writer_epoch, 0);
    assert_eq!(initial.status, ShardStatus::Active);
    assert!(
        dataset
            .list_mem_wal_latest_shard_ids()
            .await
            .unwrap()
            .contains(&shard_id),
        "the initialized shard must be discoverable through the public dataset API"
    );

    let sealed = store
        .commit_update(0, |current| lance_index::mem_wal::ShardManifest {
            version: current.version + 1,
            status: ShardStatus::Sealed,
            ..current.clone()
        })
        .await
        .unwrap();
    assert_eq!(sealed.status, ShardStatus::Sealed);
    let before_refused_claim = store.read_latest().await.unwrap().unwrap();
    let claim_error = store.claim_epoch(1).await.unwrap_err();
    assert!(claim_error.to_string().contains("sealed"));
    assert_eq!(
        store.read_latest().await.unwrap().unwrap(),
        before_refused_claim,
        "a refused claim must not mutate a sealed shard"
    );

    let active = store
        .commit_update(0, |current| lance_index::mem_wal::ShardManifest {
            version: current.version + 1,
            status: ShardStatus::Active,
            ..current.clone()
        })
        .await
        .unwrap();
    assert_eq!(active.status, ShardStatus::Active);
    let (epoch, claimed) = store.claim_epoch(1).await.unwrap();
    assert_eq!(epoch, 1);
    assert_eq!(claimed.writer_epoch, 1);
    assert_eq!(claimed.status, ShardStatus::Active);
}

async fn remove_object_store_fixture(uri: &str) {
    let (store, path) = lance_io::object_store::ObjectStore::from_uri(uri)
        .await
        .expect("configured object-store fixture must resolve for cleanup");
    store
        .remove_dir_all(path)
        .await
        .expect("configured object-store fixture cleanup must succeed");
}

/// Bucket-gated RC.1 cell for the exact backend where enrollment recovery
/// matters. CI's RustFS job sets `OMNIGRAPH_S3_TEST_BUCKET`, making this test
/// non-vacuous there; local runs without the fixture skip explicitly.
#[tokio::test(flavor = "multi_thread")]
async fn s3_memwal_enrollment_gate_positive_and_listing_negatives() {
    let Ok(bucket) = std::env::var("OMNIGRAPH_S3_TEST_BUCKET") else {
        eprintln!(
            "SKIP s3_memwal_enrollment_gate_positive_and_listing_negatives: \
             OMNIGRAPH_S3_TEST_BUCKET unset"
        );
        return;
    };
    let prefix = std::env::var("OMNIGRAPH_S3_TEST_PREFIX")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "omnigraph-itests".to_string());
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock must be after the Unix epoch")
        .as_nanos();
    let base_uri = format!(
        "s3://{bucket}/{prefix}/memwal-enrollment-gate/{}-{unique}",
        std::process::id()
    );
    let uri = format!("{base_uri}-positive.lance");

    let mut dataset = fresh_dataset(&uri).await;
    let before = capture_pre_enrollment_head(&dataset).await.unwrap();
    assert!(
        before.manifest_e_tag.is_some(),
        "configured S3/RustFS backend must expose the pre-enrollment manifest e_tag"
    );
    let expected_defaults = expected_writer_defaults(&ShardId::new_v4().to_string());
    let expected_shard_id = ShardId::new_v4();
    assert_eq!(
        classify_combined_enrollment_state(
            &before,
            &dataset,
            &expected_defaults,
            expected_shard_id,
        )
        .await
        .unwrap(),
        CombinedEnrollmentState::ExactNoEffect
    );
    initialize_with_defaults(&mut dataset, &expected_defaults)
        .await
        .unwrap();

    // Simulate a lost acknowledgement: discard the only handle that observed
    // `execute()` returning and reconstruct the result solely from durable S3.
    drop(dataset);
    let rustfs_probe = measure_exact_enrollment_probe(&uri, &before, &expected_defaults).await;
    assert!(
        rustfs_probe
            .attempts
            .iter()
            .any(|attempt| attempt.method == "head" && attempt.outcome == AttemptOutcome::NotFound),
        "RustFS exact-successor evidence must include the N+2 NotFound HEAD: {rustfs_probe:?}"
    );
    eprintln!("RustFS MemWAL exact enrollment/witness probe: {rustfs_probe:?}");
    let first_reopen = DatasetBuilder::from_uri(&uri)
        .load()
        .await
        .expect("lost-ack S3/RustFS enrollment must reopen");
    let first_receipt =
        classify_exact_enrollment_successor(&before, &first_reopen, &expected_defaults)
            .await
            .unwrap();
    assert!(first_receipt.manifest_e_tag.is_some());
    assert!(matches!(
        classify_combined_enrollment_state(
            &before,
            &first_reopen,
            &expected_defaults,
            expected_shard_id,
        )
        .await
        .unwrap(),
        CombinedEnrollmentState::ExactIndexOnly(_)
    ));
    let writer = first_reopen
        .mem_wal_writer(
            expected_shard_id,
            intended_shard_writer_config(expected_shard_id),
        )
        .await
        .expect("S3/RustFS must admit the pre-minted high-level shard");
    assert_high_level_writer_is_exact_empty(&first_reopen, &writer, expected_shard_id).await;
    assert!(matches!(
        classify_combined_enrollment_state(
            &before,
            &first_reopen,
            &expected_defaults,
            expected_shard_id,
        )
        .await
        .unwrap(),
        CombinedEnrollmentState::ExactIndexAndExpectedEmptyShard(_)
    ));
    writer
        .close()
        .await
        .expect("empty S3/RustFS shard writer must close cleanly");
    drop(first_reopen);

    let unchanged_reopen = DatasetBuilder::from_uri(&uri)
        .load()
        .await
        .expect("unchanged S3/RustFS enrollment must reopen again");
    let unchanged_receipt =
        classify_exact_enrollment_successor(&before, &unchanged_reopen, &expected_defaults)
            .await
            .unwrap();
    assert_eq!(
        first_receipt, unchanged_receipt,
        "lost-ack classification must be stable across an unchanged object-store reopen"
    );
    assert!(matches!(
        classify_combined_enrollment_state(
            &before,
            &unchanged_reopen,
            &expected_defaults,
            expected_shard_id,
        )
        .await
        .unwrap(),
        CombinedEnrollmentState::ExactIndexAndExpectedEmptyShard(_)
    ));
    drop(unchanged_reopen);

    remove_object_store_fixture(&uri).await;

    // S3/RustFS must exercise the listing-dependent refusal paths too. Local
    // negatives alone cannot prove delimiter/recursive-list behavior on the
    // backend where recovery matters.
    let foreign_uri = format!("{base_uri}-foreign-shard.lance");
    let mut foreign = fresh_dataset(&foreign_uri).await;
    let foreign_before = capture_pre_enrollment_head(&foreign).await.unwrap();
    let foreign_defaults = expected_writer_defaults("s3-foreign-shard");
    initialize_with_defaults(&mut foreign, &foreign_defaults)
        .await
        .unwrap();
    let expected_foreign_shard = ShardId::new_v4();
    let actual_foreign_shard = ShardId::new_v4();
    let foreign_writer = foreign
        .mem_wal_writer(
            actual_foreign_shard,
            intended_shard_writer_config(actual_foreign_shard),
        )
        .await
        .unwrap();
    let foreign_error = classify_combined_enrollment_state(
        &foreign_before,
        &foreign,
        &foreign_defaults,
        expected_foreign_shard,
    )
    .await
    .unwrap_err();
    assert!(foreign_error.contains("foreign, malformed, extra, or loose artifacts"));
    foreign_writer.close().await.unwrap();
    drop(foreign);
    remove_object_store_fixture(&foreign_uri).await;

    let malformed_uri = format!("{base_uri}-malformed-root.lance");
    let malformed = fresh_dataset(&malformed_uri).await;
    let malformed_before = capture_pre_enrollment_head(&malformed).await.unwrap();
    let malformed_defaults = expected_writer_defaults("s3-malformed-root");
    put_raw_mem_wal_object(&malformed, "not-a-uuid/manifest/rogue.tmp").await;
    put_raw_mem_wal_object(&malformed, "loose-object").await;
    let malformed_error = classify_combined_enrollment_state(
        &malformed_before,
        &malformed,
        &malformed_defaults,
        ShardId::new_v4(),
    )
    .await
    .unwrap_err();
    assert!(malformed_error.contains("no-effect state has MemWAL artifacts"));
    drop(malformed);
    remove_object_store_fixture(&malformed_uri).await;

    let data_uri = format!("{base_uri}-durable-wal.lance");
    let mut data_bearing = fresh_dataset(&data_uri).await;
    let data_before = capture_pre_enrollment_head(&data_bearing).await.unwrap();
    let data_defaults = expected_writer_defaults("s3-durable-wal");
    initialize_with_defaults(&mut data_bearing, &data_defaults)
        .await
        .unwrap();
    let data_shard = ShardId::new_v4();
    let data_writer = data_bearing
        .mem_wal_writer(data_shard, intended_shard_writer_config(data_shard))
        .await
        .unwrap();
    data_writer
        .put(vec![one_row(&data_bearing, "s3-wal-row", 101)])
        .await
        .unwrap();
    let data_error =
        classify_combined_enrollment_state(&data_before, &data_bearing, &data_defaults, data_shard)
            .await
            .unwrap_err();
    assert!(data_error.contains("WAL/generation/unknown manifest objects"));
    data_writer.abort().await.unwrap();
    drop(data_bearing);
    remove_object_store_fixture(&data_uri).await;

    let cursor_uri = format!("{base_uri}-cursor-progress.lance");
    let mut cursor = fresh_dataset(&cursor_uri).await;
    let cursor_before = capture_pre_enrollment_head(&cursor).await.unwrap();
    let cursor_defaults = expected_writer_defaults("s3-cursor-progress");
    let cursor_shard = ShardId::new_v4();
    initialize_and_close_empty_shard(&mut cursor, &cursor_defaults, cursor_shard).await;
    shard_manifest_store(&cursor, cursor_shard)
        .await
        .commit_update(1, |current| lance_index::mem_wal::ShardManifest {
            version: current.version + 1,
            wal_entry_position_last_seen: 1,
            ..current.clone()
        })
        .await
        .unwrap();
    let cursor_error =
        classify_combined_enrollment_state(&cursor_before, &cursor, &cursor_defaults, cursor_shard)
            .await
            .unwrap_err();
    assert!(cursor_error.contains("non-initial manifest history"));
    drop(cursor);
    remove_object_store_fixture(&cursor_uri).await;

    let corrupt_uri = format!("{base_uri}-corrupt-manifest.lance");
    let mut corrupt = fresh_dataset(&corrupt_uri).await;
    let corrupt_before = capture_pre_enrollment_head(&corrupt).await.unwrap();
    let corrupt_defaults = expected_writer_defaults("s3-corrupt-manifest");
    let corrupt_shard = ShardId::new_v4();
    initialize_and_close_empty_shard(&mut corrupt, &corrupt_defaults, corrupt_shard).await;
    put_raw_mem_wal_object(&corrupt, &shard_manifest_object_relative(corrupt_shard, 1)).await;
    let corrupt_error = classify_combined_enrollment_state(
        &corrupt_before,
        &corrupt,
        &corrupt_defaults,
        corrupt_shard,
    )
    .await
    .unwrap_err();
    assert!(corrupt_error.contains("decode") || corrupt_error.contains("protobuf"));
    drop(corrupt);
    remove_object_store_fixture(&corrupt_uri).await;
}
