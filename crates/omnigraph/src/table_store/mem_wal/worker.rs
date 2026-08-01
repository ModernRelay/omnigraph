#![allow(dead_code)]

//! Private RFC-026 Phase-B1 one-generation worker.
//!
//! This module is deliberately below graph orchestration.  The caller owns the
//! recovery barrier and durable lifecycle checks; this module owns the parts
//! which must not escape the serialized worker: exact generation accounting,
//! `put_no_wait` plus its watcher and post-durability epoch check, replay
//! classification, the RC.1 replay watermark bridge, seal/drain proof, and
//! quiesced retirement.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Display;
use std::future::Future;
use std::ops::RangeInclusive;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use arrow_array::{ArrayRef, BooleanArray, RecordBatch, UInt32Array};
use arrow_schema::Schema as ArrowSchema;
use arrow_select::take::take;
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::mem_wal::scanner::{
    FlushedGeneration as ScannerFlushedGeneration, InMemoryMemTables, LsmScanner, ShardSnapshot,
};
use lance::dataset::mem_wal::write::BatchStore;
use lance::dataset::mem_wal::{
    DatasetMemWalExt, ShardManifestStore, ShardWriter, ShardWriterConfig, schema_with_tombstone,
};
use lance_index::mem_wal::{
    FlushedGeneration, MemWalIndexDetails, ShardId, ShardManifest, ShardStatus,
};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, watch};

use crate::db::manifest::stream::{
    ClaimReceipt, RetainedShardInventoryCommitment, retained_shard_inventory_commitment,
    stream_physical_binding_digest,
};
use crate::db::manifest::stream_token::{
    STREAM_PAYLOAD_DIGEST_VERSION, STREAM_PAYLOAD_ENCODING_VERSION,
    STREAM_TOKEN_DERIVATION_VERSION, STREAM_TOKEN_WIRE_VERSION, StreamTokenAuthorityRow,
    TrustedStreamRowMetadata,
};
use crate::db::manifest::{
    STREAM_CONFIG_VERSION, StreamLifecycle, StreamLifecycleEntry, StreamPhysicalBinding,
    TableIdentity,
};
use crate::error::{OmniError, Result as OmniResult};

use super::{MAX_RETAINED_REBIND_SHARDS, UNSHARDED_SPEC_ID};

/// Own a token and an already-invoked operation until that operation settles,
/// then transfer the token to its continuation.  Deadlines may drop a waiter
/// for the spawned task, but cannot drop either owned value.
fn spawn_after_settle<T, S, C, F>(token: T, settle: S, continuation: C)
where
    T: Send + 'static,
    S: Future<Output = ()> + Send + 'static,
    C: FnOnce(T) -> F + Send + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        settle.await;
        continuation(token).await;
    });
}

const ENROLLMENT_ID_KEY: &str = "omnigraph.enrollment_id";
const CONFIG_VERSION_KEY: &str = "omnigraph.stream_config_version";
const DURABLE_WRITE_KEY: &str = "durable_write";
const MAX_WAL_BUFFER_SIZE_KEY: &str = "max_wal_buffer_size";
const MAX_WAL_FLUSH_INTERVAL_MS_KEY: &str = "max_wal_flush_interval_ms";
const MAX_MEMTABLE_SIZE_KEY: &str = "max_memtable_size";
const MAX_MEMTABLE_ROWS_KEY: &str = "max_memtable_rows";
const MAX_MEMTABLE_BATCHES_KEY: &str = "max_memtable_batches";
const MAX_UNFLUSHED_MEMTABLE_BYTES_KEY: &str = "max_unflushed_memtable_bytes";
const ENABLE_MEMTABLE_KEY: &str = "enable_memtable";

pub(crate) const B1_MAX_GENERATION_ROWS: u64 = 8_192;
pub(crate) const B1_MAX_GENERATION_ARROW_BYTES: u64 = 32 * 1024 * 1024;
pub(crate) const B1_MAX_GENERATION_BATCHES: usize = 8_192;
/// Maximum deterministic payload bytes hashed for one normalized B2 row.
/// This is intentionally larger than the Arrow generation cap because JSON
/// escaping/base64 can expand an otherwise legal physical value.
pub(crate) const B2_MAX_CANONICAL_PAYLOAD_BYTES: u64 = 64 * 1024 * 1024;
/// Conservative root reservation held while one B2 row is normalized and
/// canonically encoded: the caller Arrow batch, a possible fully materialized
/// replacement batch, and the maximum canonical payload can coexist briefly.
pub(crate) const B2_PREPROCESSING_RESERVATION_BYTES: u64 =
    2 * B1_MAX_GENERATION_ARROW_BYTES + B2_MAX_CANONICAL_PAYLOAD_BYTES;
/// The private profile preserves the minimum two-caller overlap needed to
/// revalidate a provisional authority capture while keeping preprocessing
/// memory finite root-wide.
pub(crate) const B2_MAX_PREPROCESSING_CALLS_ROOT: u64 = 2;
pub(crate) const B2_MAX_PREPROCESSING_BYTES_ROOT: u64 =
    B2_MAX_PREPROCESSING_CALLS_ROOT * B2_PREPROCESSING_RESERVATION_BYTES;
/// Maximum Arrow memory occupied by the exact current-token winner projection
/// for one generation.
pub(crate) const B2_MAX_TOKEN_PROJECTION_ARROW_BYTES: u64 = 32 * 1024 * 1024;
/// Maximum canonical JSON bytes occupied by recovery-v12's planned token rows.
pub(crate) const B2_MAX_TOKEN_RECOVERY_JSON_BYTES: u64 = 32 * 1024 * 1024;

const B1_WAL_BUFFER_BYTES: usize = 10 * 1024 * 1024;
const B1_WAL_FLUSH_INTERVAL: Duration = Duration::from_millis(100);
const B1_WAL_PERSIST_RETRIES: usize = 3;
const B1_WAL_RETRY_BASE_DELAY: Duration = Duration::from_millis(50);
const B1_MEMTABLE_BYTES: usize = 1024 * 1024 * 1024;
const B1_MEMTABLE_ROWS: usize = 8_193;
const B1_MEMTABLE_BATCHES: usize = 8_193;
const B1_MANIFEST_SCAN_BATCH_SIZE: usize = 2;
const B1_BACKPRESSURE_LOG_INTERVAL: Duration = Duration::from_secs(30);
const B1_ASYNC_INDEX_BUFFER_ROWS: usize = 10_000;
const B1_ASYNC_INDEX_INTERVAL: Duration = Duration::from_secs(1);
const B1_STATS_LOG_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum MemWalWorkerError {
    #[error("invalid RFC-026 B1 stream configuration: {reason}")]
    InvalidConfig { reason: String },
    #[error("invalid RFC-026 B1 worker state: {reason}")]
    InvalidState { reason: String },
    #[error("RFC-026 B1 worker for {key} is retiring: {reason}")]
    Retiring {
        key: StreamWorkerKey,
        reason: String,
    },
    #[error("Lance MemWAL {operation} failed: {message}")]
    Lance {
        operation: &'static str,
        message: String,
    },
    #[error("RFC-026 B1 resource limit exceeded for {resource}: actual {actual}, limit {limit}")]
    ResourceLimit {
        resource: &'static str,
        limit: u64,
        actual: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct B1WorkerLimits {
    pub(crate) max_resident_writers_root: usize,
    pub(crate) max_resident_writers_per_table: usize,
    pub(crate) max_reserved_arrow_bytes: u64,
    pub(crate) max_b2_preprocessing_bytes: u64,
    pub(crate) max_inflight_calls: usize,
    pub(crate) max_pending_generations: usize,
    pub(crate) put_deadline: Duration,
    pub(crate) seal_deadline: Duration,
    pub(crate) abort_deadline: Duration,
    pub(crate) idle_timeout: Duration,
}

impl B1WorkerLimits {
    pub(crate) fn validate(&self) -> Result<(), MemWalWorkerError> {
        let numeric = [
            ("max_resident_writers_root", self.max_resident_writers_root),
            (
                "max_resident_writers_per_table",
                self.max_resident_writers_per_table,
            ),
            ("max_inflight_calls", self.max_inflight_calls),
            ("max_pending_generations", self.max_pending_generations),
        ];
        if let Some((field, _)) = numeric.into_iter().find(|(_, value)| *value == 0) {
            return Err(MemWalWorkerError::config(format!(
                "{field} must be non-zero"
            )));
        }
        if self.max_resident_writers_per_table > self.max_resident_writers_root {
            return Err(MemWalWorkerError::config(
                "per-table resident-writer limit cannot exceed the root limit",
            ));
        }
        if self.max_reserved_arrow_bytes < B1_MAX_GENERATION_ARROW_BYTES {
            return Err(MemWalWorkerError::config(format!(
                "aggregate Arrow reservation must fit at least one legal generation ({B1_MAX_GENERATION_ARROW_BYTES} bytes)"
            )));
        }
        if self.max_b2_preprocessing_bytes < B2_PREPROCESSING_RESERVATION_BYTES {
            return Err(MemWalWorkerError::config(format!(
                "B2 preprocessing reservation must fit one legal row's bounded scratch ({B2_PREPROCESSING_RESERVATION_BYTES} bytes)"
            )));
        }
        for (field, duration) in [
            ("put_deadline", self.put_deadline),
            ("seal_deadline", self.seal_deadline),
            ("abort_deadline", self.abort_deadline),
            ("idle_timeout", self.idle_timeout),
        ] {
            if duration.is_zero() {
                return Err(MemWalWorkerError::config(format!(
                    "{field} must be non-zero"
                )));
            }
        }
        Ok(())
    }
}

impl MemWalWorkerError {
    fn config(reason: impl Into<String>) -> Self {
        Self::InvalidConfig {
            reason: reason.into(),
        }
    }

    fn state(reason: impl Into<String>) -> Self {
        Self::InvalidState {
            reason: reason.into(),
        }
    }

    fn lance(operation: &'static str, error: impl Display) -> Self {
        Self::Lance {
            operation,
            message: error.to_string(),
        }
    }
}

/// Exact process-local identity of one physical B1 worker.
///
/// This is intentionally not an admission-lock key.  Admission remains keyed
/// by table identity plus resolved physical ref in `WriteQueueManager`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct StreamWorkerKey {
    pub(crate) identity: TableIdentity,
    pub(crate) enrollment_id: ShardId,
    pub(crate) shard_id: ShardId,
}

impl StreamWorkerKey {
    pub(crate) fn new(
        identity: TableIdentity,
        enrollment_id: ShardId,
        shard_id: ShardId,
    ) -> Result<Self, MemWalWorkerError> {
        identity
            .validate()
            .map_err(|error| MemWalWorkerError::config(error.to_string()))?;
        if enrollment_id.is_nil() || enrollment_id.get_version_num() != 4 {
            return Err(MemWalWorkerError::config(
                "enrollment_id must be a non-nil UUID v4",
            ));
        }
        if shard_id.is_nil() || shard_id.get_version_num() != 4 {
            return Err(MemWalWorkerError::config(
                "shard_id must be a non-nil UUID v4",
            ));
        }
        if enrollment_id == shard_id {
            return Err(MemWalWorkerError::config(
                "enrollment_id and shard_id must be distinct",
            ));
        }
        Ok(Self {
            identity,
            enrollment_id,
            shard_id,
        })
    }
}

impl std::fmt::Display for StreamWorkerKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}:{}",
            self.identity, self.enrollment_id, self.shard_id
        )
    }
}

/// Stable, non-empty caller ordinal interval for one normalized batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CallerOrdinalRange {
    pub(crate) start: u64,
    pub(crate) end: u64,
}

impl CallerOrdinalRange {
    pub(crate) fn new(start: u64, end: u64) -> Result<Self, MemWalWorkerError> {
        if start > end {
            return Err(MemWalWorkerError::state(
                "caller ordinal range must be non-empty and increasing",
            ));
        }
        Ok(Self { start, end })
    }

    fn len(self) -> Result<u64, MemWalWorkerError> {
        self.end
            .checked_sub(self.start)
            .and_then(|distance| distance.checked_add(1))
            .ok_or_else(|| MemWalWorkerError::state("caller ordinal range length overflow"))
    }

    fn as_range(self) -> RangeInclusive<u64> {
        self.start..=self.end
    }
}

/// Status-only durability proof.  It deliberately contains no WAL, generation,
/// or MemTable batch coordinate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DurableBatchAck {
    pub(crate) identity: TableIdentity,
    pub(crate) enrollment_id: ShardId,
    pub(crate) shard_id: ShardId,
    pub(crate) writer_epoch: u64,
    pub(crate) caller_ordinals: CallerOrdinalRange,
    pub(crate) row_count: u64,
}

/// Watcher-confirmed same-generation sequencing state for one logical key.
///
/// This is deliberately a warm projection, never restart authority.  It is
/// installed only after the durability watcher and the same writer's final
/// epoch check both succeed, and it disappears with the resident worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConfirmedStreamTokenOverlayRow {
    pub(crate) authority: StreamTokenAuthorityRow,
    pub(crate) metadata: TrustedStreamRowMetadata,
}

impl ConfirmedStreamTokenOverlayRow {
    pub(crate) fn validate(&self, identity: TableIdentity, logical_id: &str) -> OmniResult<()> {
        self.authority
            .validate()
            .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
        self.metadata
            .validate_for(identity, logical_id)
            .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
        if self.authority.identity != identity
            || self.authority.logical_id != logical_id
            || !self.metadata.agrees_with_authority(&self.authority)
        {
            return Err(OmniError::manifest_internal(format!(
                "confirmed stream-token overlay for '{logical_id}' disagrees with its table identity or trusted metadata"
            )));
        }
        Ok(())
    }
}

pub(crate) type ConfirmedStreamTokenOverlay = BTreeMap<String, ConfirmedStreamTokenOverlayRow>;

/// Opaque proof that the background-owned append task owns the shared
/// admission lease after completing the caller's fresh authority check.
pub(crate) struct CheckedStreamAuthority {
    _guard: OwnedRwLockReadGuard<()>,
    _profile_guard: Option<OwnedRwLockReadGuard<()>>,
}

impl CheckedStreamAuthority {
    pub(crate) fn from_shared_admission(guard: OwnedRwLockReadGuard<()>) -> Self {
        Self {
            _guard: guard,
            _profile_guard: None,
        }
    }

    /// Carry graph-profile authority into the detached append tail together
    /// with the table admission lease.
    ///
    /// Production stream admission acquires profile-shared before the table
    /// lease. Keeping both guards in this opaque token prevents a cancelled
    /// request from releasing profile authority while Lance's put watcher or
    /// same-writer fence check is still settling.
    pub(crate) fn from_shared_admission_with_profile(
        guard: OwnedRwLockReadGuard<()>,
        profile_guard: OwnedRwLockReadGuard<()>,
    ) -> Self {
        Self {
            _guard: guard,
            _profile_guard: Some(profile_guard),
        }
    }
}

/// Opaque proof that a background-owned fold task owns the exclusive
/// admission lease.  A successful cut carries this token onward so the fold
/// sidecar/table/manifest sequence cannot reopen an admission gap.
pub(crate) struct CheckedExclusiveStreamAuthority {
    _guard: OwnedRwLockWriteGuard<()>,
}

impl CheckedExclusiveStreamAuthority {
    pub(crate) fn from_exclusive_admission(guard: OwnedRwLockWriteGuard<()>) -> Self {
        Self { _guard: guard }
    }
}

pub(super) fn expected_b1_writer_defaults(enrollment_id: ShardId) -> BTreeMap<String, String> {
    BTreeMap::from([
        (ENROLLMENT_ID_KEY.to_string(), enrollment_id.to_string()),
        (
            CONFIG_VERSION_KEY.to_string(),
            STREAM_CONFIG_VERSION.to_string(),
        ),
        (DURABLE_WRITE_KEY.to_string(), "true".to_string()),
        (
            MAX_WAL_BUFFER_SIZE_KEY.to_string(),
            B1_WAL_BUFFER_BYTES.to_string(),
        ),
        (
            MAX_WAL_FLUSH_INTERVAL_MS_KEY.to_string(),
            B1_WAL_FLUSH_INTERVAL.as_millis().to_string(),
        ),
        (
            MAX_MEMTABLE_SIZE_KEY.to_string(),
            B1_MEMTABLE_BYTES.to_string(),
        ),
        (
            MAX_MEMTABLE_ROWS_KEY.to_string(),
            B1_MEMTABLE_ROWS.to_string(),
        ),
        (
            MAX_MEMTABLE_BATCHES_KEY.to_string(),
            B1_MEMTABLE_BATCHES.to_string(),
        ),
        (
            MAX_UNFLUSHED_MEMTABLE_BYTES_KEY.to_string(),
            B1_MEMTABLE_BYTES.to_string(),
        ),
        (ENABLE_MEMTABLE_KEY.to_string(), "true".to_string()),
    ])
}

fn canonical_b1_stream_config_v2() -> String {
    format!(
        "omnigraph.stream_config_version=2\n\
         sharding=unsharded\n\
         num_shards=1\n\
         shard_spec_id={UNSHARDED_SPEC_ID}\n\
         shard_field_id=bucket\n\
         shard_result_type=int32\n\
         maintained_indexes=\n\
         durable_write=true\n\
         max_wal_buffer_size={B1_WAL_BUFFER_BYTES}\n\
         max_wal_flush_interval_ms={}\n\
         max_memtable_size={B1_MEMTABLE_BYTES}\n\
         max_memtable_rows={B1_MEMTABLE_ROWS}\n\
         max_memtable_batches={B1_MEMTABLE_BATCHES}\n\
         max_unflushed_memtable_bytes={B1_MEMTABLE_BYTES}\n\
         enable_memtable=true\n",
        B1_WAL_FLUSH_INTERVAL.as_millis(),
    )
}

/// Historical config-v2 digest used only to parse/refuse recovery-v11 files.
/// Active enrollment and fold code always emits config v3.
pub(crate) fn stream_config_v2_hash() -> String {
    let digest = Sha256::digest(canonical_b1_stream_config_v2().as_bytes());
    format!("sha256:{digest:x}")
}

fn canonical_b2_stream_config() -> String {
    format!(
        "omnigraph.stream_config_version={STREAM_CONFIG_VERSION}\n\
         sharding=unsharded\n\
         num_shards=1\n\
         shard_spec_id={UNSHARDED_SPEC_ID}\n\
         shard_field_id=bucket\n\
         shard_result_type=int32\n\
         maintained_indexes=\n\
         durable_write=true\n\
         max_wal_buffer_size={B1_WAL_BUFFER_BYTES}\n\
         max_wal_flush_interval_ms={}\n\
         max_memtable_size={B1_MEMTABLE_BYTES}\n\
         max_memtable_rows={B1_MEMTABLE_ROWS}\n\
         max_memtable_batches={B1_MEMTABLE_BATCHES}\n\
         max_unflushed_memtable_bytes={B1_MEMTABLE_BYTES}\n\
         enable_memtable=true\n\
         stream_token_derivation_version={STREAM_TOKEN_DERIVATION_VERSION}\n\
         stream_token_wire={STREAM_TOKEN_WIRE_VERSION}\n\
         payload_digest_version={STREAM_PAYLOAD_DIGEST_VERSION}\n\
         payload_encoding_version={STREAM_PAYLOAD_ENCODING_VERSION}\n\
         max_canonical_payload_bytes={B2_MAX_CANONICAL_PAYLOAD_BYTES}\n\
         max_token_projection_arrow_bytes={B2_MAX_TOKEN_PROJECTION_ARROW_BYTES}\n\
         max_token_recovery_json_bytes={B2_MAX_TOKEN_RECOVERY_JSON_BYTES}\n",
        B1_WAL_FLUSH_INTERVAL.as_millis(),
    )
}

pub(crate) fn stream_config_v3_hash() -> String {
    let digest = Sha256::digest(canonical_b2_stream_config().as_bytes());
    format!("sha256:{digest:x}")
}

fn validate_b1_topology(
    details: &MemWalIndexDetails,
    enrollment_id: ShardId,
) -> Result<(), MemWalWorkerError> {
    if details.num_shards != 1
        || details.sharding_specs.len() != 1
        || !details.maintained_indexes.is_empty()
    {
        return Err(MemWalWorkerError::config(format!(
            "expected one unsharded shard and no maintained indexes, got num_shards={}, specs={}, maintained_indexes={:?}",
            details.num_shards,
            details.sharding_specs.len(),
            details.maintained_indexes
        )));
    }
    let spec = &details.sharding_specs[0];
    if spec.spec_id != UNSHARDED_SPEC_ID || spec.fields.len() != 1 {
        return Err(MemWalWorkerError::config(
            "unexpected unsharded specification identity",
        ));
    }
    let field = &spec.fields[0];
    if field.field_id != "bucket"
        || !field.source_ids.is_empty()
        || field.transform.as_deref() != Some("unsharded")
        || field.expression.is_some()
        || field.result_type != "int32"
        || !field.parameters.is_empty()
    {
        return Err(MemWalWorkerError::config(
            "unexpected unsharded specification field",
        ));
    }
    let actual = details
        .writer_config_defaults
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    let expected = expected_b1_writer_defaults(enrollment_id);
    if actual != expected {
        return Err(MemWalWorkerError::config(format!(
            "persisted config-v3 defaults differ: expected={expected:?}, actual={actual:?}"
        )));
    }
    Ok(())
}

/// B1 binds one physical shard.  Merge progress for any other shard is
/// therefore foreign state, not an ignorable cursor.  Reject it before a
/// writer can be claimed or a fold can be staged.
fn validate_bound_merge_progress(
    details: &MemWalIndexDetails,
    shard_id: ShardId,
) -> Result<(), MemWalWorkerError> {
    let foreign = details
        .merged_generations
        .iter()
        .filter(|merged| merged.shard_id != shard_id)
        .map(|merged| (merged.shard_id, merged.generation))
        .collect::<Vec<_>>();
    if !foreign.is_empty() {
        return Err(MemWalWorkerError::state(format!(
            "merged-generation progress contains entries outside bound shard {shard_id}: {foreign:?}"
        )));
    }
    Ok(())
}

/// Validate that durable graph binding identity and Lance's persisted config-v3
/// describe exactly the same physical enrollment.  The enrollment UUID is
/// never inferred from the replaceable MemWAL index UUID.
pub(crate) fn validate_stream_config_v3_binding(
    details: &MemWalIndexDetails,
    binding: &StreamPhysicalBinding,
) -> Result<(ShardId, ShardId), MemWalWorkerError> {
    if binding.stream_config_version != STREAM_CONFIG_VERSION {
        return Err(MemWalWorkerError::config(format!(
            "binding config version is {}, expected {STREAM_CONFIG_VERSION}",
            binding.stream_config_version
        )));
    }
    if binding.stream_config_hash != stream_config_v3_hash() {
        return Err(MemWalWorkerError::config(format!(
            "binding config hash is {}, expected {}",
            binding.stream_config_hash,
            stream_config_v3_hash()
        )));
    }
    if binding.table_branch.is_some() || binding.shard_ids.len() != 1 {
        return Err(MemWalWorkerError::config(
            "B1 supports only main and exactly one shard",
        ));
    }
    let enrollment_id = ShardId::parse_str(&binding.enrollment_id)
        .map_err(|error| MemWalWorkerError::config(format!("invalid enrollment UUID: {error}")))?;
    let shard_id = ShardId::parse_str(&binding.shard_ids[0])
        .map_err(|error| MemWalWorkerError::config(format!("invalid shard UUID: {error}")))?;
    StreamWorkerKey::new(
        binding
            .identity()
            .map_err(|error| MemWalWorkerError::config(error.to_string()))?,
        enrollment_id,
        shard_id,
    )?;
    validate_b1_topology(details, enrollment_id)?;
    validate_bound_merge_progress(details, shard_id)?;
    Ok((enrollment_id, shard_id))
}

/// Reconstruct every scalar `ShardWriterConfig` field explicitly.  Runtime
/// capabilities (`Session` and store params) are injected by Lance's
/// `Dataset::mem_wal_writer`; `warmer=None` is pinned here.
pub(crate) fn reconstruct_b1_writer_config(
    details: &MemWalIndexDetails,
    enrollment_id: ShardId,
    shard_id: ShardId,
) -> Result<ShardWriterConfig, MemWalWorkerError> {
    validate_b1_topology(details, enrollment_id)?;
    validate_bound_merge_progress(details, shard_id)?;
    let mut config = ShardWriterConfig::new(shard_id);
    config.shard_spec_id = UNSHARDED_SPEC_ID;
    config.durable_write = true;
    config.sync_indexed_write = true;
    config.max_wal_buffer_size = B1_WAL_BUFFER_BYTES;
    config.max_wal_flush_interval = Some(B1_WAL_FLUSH_INTERVAL);
    config.max_wal_persist_retries = B1_WAL_PERSIST_RETRIES;
    config.wal_persist_retry_base_delay = B1_WAL_RETRY_BASE_DELAY;
    config.max_memtable_size = B1_MEMTABLE_BYTES;
    config.max_memtable_rows = B1_MEMTABLE_ROWS;
    config.max_memtable_batches = B1_MEMTABLE_BATCHES;
    config.manifest_scan_batch_size = B1_MANIFEST_SCAN_BATCH_SIZE;
    config.max_unflushed_memtable_bytes = B1_MEMTABLE_BYTES;
    config.backpressure_log_interval = B1_BACKPRESSURE_LOG_INTERVAL;
    config.async_index_buffer_rows = B1_ASYNC_INDEX_BUFFER_ROWS;
    config.async_index_interval = B1_ASYNC_INDEX_INTERVAL;
    config.stats_log_interval = Some(B1_STATS_LOG_INTERVAL);
    config.frozen_memtable_grace = Duration::ZERO;
    config.enable_memtable = true;
    config.hnsw_params.clear();
    config.warmer = None;
    config.store_params = None;
    config.session = None;
    Ok(config)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PassiveB1PhysicalState {
    /// The shard manifest has no unmerged flushed generation. Durable WAL may
    /// still replay into the active MemTable when the first writer claims it.
    AdmitOrReplay {
        shard_manifest_version: u64,
        current_generation: u64,
        replay_after_wal_entry_position: u64,
        writer_epoch: u64,
    },
    FoldOnlyFlushed(FlushedGenerationState),
}

fn exact_unmerged_generations(
    manifest: &ShardManifest,
    details: &MemWalIndexDetails,
) -> Result<(u64, Vec<FlushedGeneration>), MemWalWorkerError> {
    let merged = merged_generation_for_bound_shard(details, manifest.shard_id)?;
    let mut seen_generations = std::collections::BTreeSet::new();
    let mut seen_paths = std::collections::BTreeSet::new();
    for generation in &manifest.flushed_generations {
        if generation.generation == 0
            || generation.generation >= manifest.current_generation
            || generation.path.is_empty()
            || !seen_generations.insert(generation.generation)
            || !seen_paths.insert(generation.path.as_str())
        {
            return Err(MemWalWorkerError::state(format!(
                "invalid flushed-generation topology in shard manifest: {:?}",
                manifest.flushed_generations
            )));
        }
    }
    let observed_generations = seen_generations.iter().copied().collect::<Vec<_>>();
    let expected_generations = (1..manifest.current_generation).collect::<Vec<_>>();
    if observed_generations != expected_generations {
        return Err(MemWalWorkerError::state(format!(
            "flushed-generation history has gaps: expected {expected_generations:?}, observed {observed_generations:?}"
        )));
    }
    if merged >= manifest.current_generation {
        return Err(MemWalWorkerError::state(format!(
            "merged generation {merged} is not below current generation {}",
            manifest.current_generation
        )));
    }
    let mut unmerged = manifest
        .flushed_generations
        .iter()
        .filter(|generation| generation.generation > merged)
        .cloned()
        .collect::<Vec<_>>();
    unmerged.sort_by_key(|generation| generation.generation);
    if unmerged.len() > 1 {
        return Err(MemWalWorkerError::state(format!(
            "expected at most one unmerged generation, observed {:?}",
            unmerged
                .iter()
                .map(|generation| generation.generation)
                .collect::<Vec<_>>()
        )));
    }
    let expected_current = merged
        .checked_add(1)
        .and_then(|next| next.checked_add(u64::from(!unmerged.is_empty())))
        .ok_or_else(|| MemWalWorkerError::state("generation topology overflow"))?;
    if manifest.current_generation != expected_current {
        return Err(MemWalWorkerError::state(format!(
            "current generation {} differs from exact merged/unmerged successor {expected_current}",
            manifest.current_generation
        )));
    }
    if let Some(unmerged) = unmerged.first()
        && unmerged.generation != merged.saturating_add(1)
    {
        return Err(MemWalWorkerError::state(format!(
            "unmerged generation {} is not the exact successor of merged generation {merged}",
            unmerged.generation
        )));
    }
    Ok((merged, unmerged))
}

/// Passive read/open-time validation for a data-bearing config-v3 lifecycle.
///
/// Unlike [`classify_active_state`], this never calls `mem_wal_writer` and
/// therefore never claims an epoch. It validates only durable/public state;
/// whether a WAL tail replays into a non-empty active MemTable is deliberately
/// deferred to the claimed worker classifier.
pub(crate) async fn validate_b1_lifecycle_physical_state(
    dataset: &Dataset,
    lifecycle: &StreamLifecycleEntry,
) -> Result<PassiveB1PhysicalState, MemWalWorkerError> {
    validate_b1_lifecycle_physical_state_impl(
        dataset,
        lifecycle,
        None,
        PassiveBindingInventoryValidation::CompleteRetainedPrefixes,
    )
    .await
}

/// Passive validation using the fixed-size retained-prefix commitment from the
/// manifest-selected binding receipt. Only the lifecycle's current shard is
/// opened or interpreted; older prefixes remain inert retained history.
pub(crate) async fn validate_b1_lifecycle_physical_state_with_binding_inventory(
    dataset: &Dataset,
    lifecycle: &StreamLifecycleEntry,
    retained_inventory: Option<&RetainedShardInventoryCommitment>,
) -> Result<PassiveB1PhysicalState, MemWalWorkerError> {
    validate_b1_lifecycle_physical_state_impl(
        dataset,
        lifecycle,
        retained_inventory,
        PassiveBindingInventoryValidation::CompleteRetainedPrefixes,
    )
    .await
}

/// Hot-path passive validation for the selected current binding only.
///
/// The caller must first authenticate `retained_inventory` from the
/// manifest-selected `BindingReceipt`. Historical shard prefixes are inert and
/// do not participate in writer admission, so ordinary capture must not relist
/// them on every request. Cold open, rebind, and recovery continue to use
/// [`validate_b1_lifecycle_physical_state_with_binding_inventory`] to compare
/// the complete physical prefix set with that fixed-size commitment.
pub(crate) async fn validate_b1_lifecycle_current_binding_physical_state(
    dataset: &Dataset,
    lifecycle: &StreamLifecycleEntry,
    retained_inventory: Option<&RetainedShardInventoryCommitment>,
) -> Result<PassiveB1PhysicalState, MemWalWorkerError> {
    validate_b1_lifecycle_physical_state_impl(
        dataset,
        lifecycle,
        retained_inventory,
        PassiveBindingInventoryValidation::CurrentBindingOnly,
    )
    .await
}

#[derive(Clone, Copy)]
enum PassiveBindingInventoryValidation {
    CurrentBindingOnly,
    CompleteRetainedPrefixes,
}

async fn validate_b1_lifecycle_physical_state_impl(
    dataset: &Dataset,
    lifecycle: &StreamLifecycleEntry,
    retained_inventory: Option<&RetainedShardInventoryCommitment>,
    inventory_validation: PassiveBindingInventoryValidation,
) -> Result<PassiveB1PhysicalState, MemWalWorkerError> {
    lifecycle
        .validate()
        .map_err(|error| MemWalWorkerError::config(error.to_string()))?;
    if dataset.version().version != lifecycle.current_head_witness.table_version {
        return Err(MemWalWorkerError::state(format!(
            "opened table version {} differs from lifecycle witness {}",
            dataset.version().version,
            lifecycle.current_head_witness.table_version
        )));
    }
    let observed = super::capture_current_head_witness(dataset)
        .await
        .map_err(|error| MemWalWorkerError::state(error.to_string()))?;
    if observed != lifecycle.current_head_witness {
        return Err(MemWalWorkerError::state(format!(
            "opened table witness differs from lifecycle: expected={:?}, observed={observed:?}",
            lifecycle.current_head_witness
        )));
    }
    let details = dataset
        .mem_wal_index_details()
        .await
        .map_err(|error| MemWalWorkerError::lance("passive config read", error))?
        .ok_or_else(|| MemWalWorkerError::state("lifecycle-bound table has no MemWAL index"))?;
    let (_, shard_id) = validate_stream_config_v3_binding(&details, &lifecycle.binding)?;
    require_selected_passive_shard_commitment(
        lifecycle.binding_receipt_chain.record_count,
        retained_inventory,
    )?;
    let object_store = dataset
        .object_store(None)
        .await
        .map_err(|error| MemWalWorkerError::lance("passive shard store open", error))?;
    if matches!(
        inventory_validation,
        PassiveBindingInventoryValidation::CompleteRetainedPrefixes
    ) {
        let mut shard_ids = dataset
            .list_mem_wal_latest_shard_ids()
            .await
            .map_err(|error| MemWalWorkerError::lance("passive shard listing", error))?;
        shard_ids.sort_unstable();
        require_committed_passive_shard_inventory(
            lifecycle.binding_receipt_chain.record_count,
            shard_id,
            retained_inventory,
            &shard_ids,
            "physical shard namespace",
        )?;

        let mem_wal_root = dataset.branch_location().path.join("_mem_wal");
        let top_level = object_store
            .inner
            .list_with_delimiter(Some(&mem_wal_root))
            .await
            .map_err(|error| MemWalWorkerError::lance("passive MemWAL root listing", error))?;
        if !top_level.objects.is_empty() {
            return Err(MemWalWorkerError::state(format!(
                "MemWAL root contains loose objects: {:?}",
                top_level
                    .objects
                    .iter()
                    .map(|object| object.location.as_ref())
                    .collect::<Vec<_>>()
            )));
        }
        let mut top_level_shards = top_level
            .common_prefixes
            .iter()
            .map(|prefix| {
                let name = prefix.filename().ok_or_else(|| {
                    MemWalWorkerError::state(format!(
                        "MemWAL root contains an unnamed prefix {prefix:?}"
                    ))
                })?;
                ShardId::parse_str(name).map_err(|error| {
                    MemWalWorkerError::state(format!(
                        "MemWAL root contains malformed shard prefix {name:?}: {error}"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        top_level_shards.sort();
        require_committed_passive_shard_inventory(
            lifecycle.binding_receipt_chain.record_count,
            shard_id,
            retained_inventory,
            &top_level_shards,
            "MemWAL root prefixes",
        )?;
    }
    let manifest = ShardManifestStore::new(
        object_store,
        &dataset.branch_location().path,
        shard_id,
        B1_MANIFEST_SCAN_BATCH_SIZE,
    )
    .read_latest()
    .await
    .map_err(|error| MemWalWorkerError::lance("passive shard manifest read", error))?
    .ok_or_else(|| MemWalWorkerError::state("bound shard has no manifest"))?;
    let epoch_floor = lifecycle
        .epoch_floor_by_shard
        .get(&shard_id.to_string())
        .copied()
        .ok_or_else(|| MemWalWorkerError::state("lifecycle has no bound shard epoch floor"))?;
    if manifest.shard_id != shard_id
        || manifest.shard_spec_id != UNSHARDED_SPEC_ID
        || manifest.status != ShardStatus::Active
        || !manifest.shard_field_values.is_empty()
        || manifest.writer_epoch < epoch_floor
        || manifest.writer_epoch == 0
    {
        return Err(MemWalWorkerError::state(format!(
            "bound shard manifest violates identity/status/epoch floor {epoch_floor}: {manifest:?}"
        )));
    }
    let (_, unmerged) = exact_unmerged_generations(&manifest, &details)?;
    match unmerged.as_slice() {
        [] => Ok(PassiveB1PhysicalState::AdmitOrReplay {
            shard_manifest_version: manifest.version,
            current_generation: manifest.current_generation,
            replay_after_wal_entry_position: manifest.replay_after_wal_entry_position,
            writer_epoch: manifest.writer_epoch,
        }),
        [generation] => {
            if manifest.replay_after_wal_entry_position == 0 {
                return Err(MemWalWorkerError::state(
                    "flushed generation has zero authoritative WAL cursor",
                ));
            }
            Ok(PassiveB1PhysicalState::FoldOnlyFlushed(
                FlushedGenerationState {
                    shard_manifest_version: manifest.version,
                    writer_epoch: manifest.writer_epoch,
                    generation: generation.generation,
                    path: generation.path.clone(),
                    replay_after_wal_entry_position: manifest.replay_after_wal_entry_position,
                },
            ))
        }
        _ => unreachable!("exact_unmerged_generations bounded the result"),
    }
}

fn require_selected_passive_shard_commitment(
    binding_receipt_record_count: u64,
    retained_inventory: Option<&RetainedShardInventoryCommitment>,
) -> Result<(), MemWalWorkerError> {
    let Some(commitment) = retained_inventory else {
        if binding_receipt_record_count != 2 {
            return Err(MemWalWorkerError::state(
                "strict initial-binding validation cannot accept a rebound lifecycle without a retained-shard commitment",
            ));
        }
        return Ok(());
    };
    if binding_receipt_record_count <= 2 {
        return Err(MemWalWorkerError::state(
            "initial binding cannot carry a retained-shard commitment",
        ));
    }
    let chain_derived_count = binding_receipt_record_count.checked_sub(1).ok_or_else(|| {
        MemWalWorkerError::state("binding receipt retained-shard count underflow")
    })?;
    if commitment.retained_shard_count != chain_derived_count
        || commitment.retained_shard_count == 0
        || commitment.retained_shard_count > MAX_RETAINED_REBIND_SHARDS as u64
    {
        return Err(MemWalWorkerError::state(format!(
            "selected retained-shard count {}, chain-derived count {chain_derived_count}, and maximum {MAX_RETAINED_REBIND_SHARDS} disagree",
            commitment.retained_shard_count,
        )));
    }
    Ok(())
}

fn require_committed_passive_shard_inventory(
    binding_receipt_record_count: u64,
    current_shard: ShardId,
    retained_inventory: Option<&RetainedShardInventoryCommitment>,
    observed: &[ShardId],
    source: &'static str,
) -> Result<(), MemWalWorkerError> {
    require_selected_passive_shard_commitment(binding_receipt_record_count, retained_inventory)?;
    let Some(commitment) = retained_inventory else {
        if observed != [current_shard] {
            return Err(MemWalWorkerError::state(format!(
                "{source} differs from the exact initial singleton binding: expected [{current_shard}], observed {observed:?}"
            )));
        }
        return Ok(());
    };
    if observed.is_empty() || observed.len() > MAX_RETAINED_REBIND_SHARDS {
        return Err(MemWalWorkerError::state(format!(
            "{source} must contain 1..={MAX_RETAINED_REBIND_SHARDS} shard prefixes"
        )));
    }
    if observed.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(MemWalWorkerError::state(format!(
            "{source} is not strictly sorted and duplicate-free"
        )));
    }
    if observed.binary_search(&current_shard).is_err() {
        return Err(MemWalWorkerError::state(format!(
            "{source} omits current shard {current_shard}"
        )));
    }
    let chain_derived_count = binding_receipt_record_count - 1;
    let observed_count = u64::try_from(observed.len())
        .map_err(|_| MemWalWorkerError::state("observed shard-prefix count overflow"))?;
    if commitment.retained_shard_count != chain_derived_count
        || observed_count != commitment.retained_shard_count
    {
        return Err(MemWalWorkerError::state(format!(
            "{source} count {observed_count}, committed count {}, and chain-derived count {chain_derived_count} disagree",
            commitment.retained_shard_count,
        )));
    }
    let observed_commitment = retained_shard_inventory_commitment(observed)
        .map_err(|error| MemWalWorkerError::state(error.to_string()))?;
    if &observed_commitment != commitment {
        return Err(MemWalWorkerError::state(format!(
            "{source} differs from its selected binding-receipt commitment"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct GenerationAccounting {
    pub(crate) rows: u64,
    pub(crate) arrow_bytes: u64,
    pub(crate) batches: usize,
}

fn full_generation_reservation() -> GenerationAccounting {
    GenerationAccounting {
        rows: 0,
        arrow_bytes: B1_MAX_GENERATION_ARROW_BYTES,
        batches: 0,
    }
}

impl GenerationAccounting {
    fn checked_add(self, other: Self) -> Result<Self, MemWalWorkerError> {
        Ok(Self {
            rows: self
                .rows
                .checked_add(other.rows)
                .ok_or_else(|| MemWalWorkerError::state("generation row accounting overflow"))?,
            arrow_bytes: self
                .arrow_bytes
                .checked_add(other.arrow_bytes)
                .ok_or_else(|| {
                    MemWalWorkerError::state("generation Arrow-byte accounting overflow")
                })?,
            batches: self
                .batches
                .checked_add(other.batches)
                .ok_or_else(|| MemWalWorkerError::state("generation batch accounting overflow"))?,
        })
    }

    fn checked_sub(self, other: Self) -> Result<Self, MemWalWorkerError> {
        Ok(Self {
            rows: self
                .rows
                .checked_sub(other.rows)
                .ok_or_else(|| MemWalWorkerError::state("generation row accounting underflow"))?,
            arrow_bytes: self
                .arrow_bytes
                .checked_sub(other.arrow_bytes)
                .ok_or_else(|| {
                    MemWalWorkerError::state("generation Arrow-byte accounting underflow")
                })?,
            batches: self
                .batches
                .checked_sub(other.batches)
                .ok_or_else(|| MemWalWorkerError::state("generation batch accounting underflow"))?,
        })
    }

    pub(crate) fn fits(self) -> bool {
        self.rows <= B1_MAX_GENERATION_ROWS
            && self.arrow_bytes <= B1_MAX_GENERATION_ARROW_BYTES
            && self.batches <= B1_MAX_GENERATION_BATCHES
    }
}

fn account_batch(batch: &RecordBatch) -> Result<GenerationAccounting, MemWalWorkerError> {
    let rows = u64::try_from(batch.num_rows())
        .map_err(|_| MemWalWorkerError::state("batch row count does not fit u64"))?;
    let arrow_bytes = b1_logical_batch_bytes(batch)?;
    Ok(GenerationAccounting {
        rows,
        arrow_bytes,
        batches: 1,
    })
}

/// Stable logical Arrow bytes for one normalized generation batch.
///
/// Lance replay/scanner arrays may be slices whose backing buffers have much
/// larger capacities than the selected rows. Charging `get_array_memory_size`
/// would therefore make the same durable generation legal before a restart
/// and corrupt after it. The RFC-026 limit is the memory needed by dense buffers
/// for the selected slice; allocator/RSS amplification is measured separately.
pub(crate) fn b1_logical_batch_bytes(batch: &RecordBatch) -> Result<u64, MemWalWorkerError> {
    batch.columns().iter().try_fold(0_u64, |total, column| {
        let bytes = column.to_data().get_slice_memory_size().map_err(|error| {
            MemWalWorkerError::state(format!("logical Arrow slice accounting failed: {error}"))
        })?;
        total
            .checked_add(u64::try_from(bytes).map_err(|_| {
                MemWalWorkerError::state("logical Arrow slice size does not fit u64")
            })?)
            .ok_or_else(|| MemWalWorkerError::state("logical Arrow slice size overflow"))
    })
}

fn account_store(store: &BatchStore) -> Result<GenerationAccounting, MemWalWorkerError> {
    store
        .iter()
        .try_fold(GenerationAccounting::default(), |total, stored| {
            total.checked_add(account_batch(&stored.data)?)
        })
}

fn post_tombstone_accounting(
    batch: &RecordBatch,
) -> Result<GenerationAccounting, MemWalWorkerError> {
    if batch.num_rows() == 0 {
        return Err(MemWalWorkerError::state("B1 rejects an empty batch"));
    }
    if batch
        .column_by_name(lance::dataset::mem_wal::TOMBSTONE)
        .is_some()
    {
        return Err(MemWalWorkerError::state(
            "normalized caller batch must not contain Lance's reserved _tombstone column",
        ));
    }
    let schema = schema_with_tombstone(batch.schema().as_ref());
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(BooleanArray::from(vec![false; batch.num_rows()])) as ArrayRef);
    let stored = RecordBatch::try_new(schema, columns)
        .map_err(|error| MemWalWorkerError::state(format!("tombstone accounting: {error}")))?;
    account_batch(&stored)
}

/// Bound caller-owned input before the registry takes ownership or starts a
/// detached cold-claim task.  The cheap raw check runs first so a maliciously
/// large row count cannot force an equally large tombstone allocation merely
/// to discover that the batch is over the B1 cap.  Inputs which can still fit
/// are charged using Lance's exact post-tombstone representation.
pub(crate) fn b1_input_accounting(
    batch: &RecordBatch,
) -> Result<GenerationAccounting, MemWalWorkerError> {
    if batch.num_rows() == 0
        || batch
            .column_by_name(lance::dataset::mem_wal::TOMBSTONE)
            .is_some()
    {
        return post_tombstone_accounting(batch);
    }
    let raw = account_batch(batch)?;
    if raw.rows > B1_MAX_GENERATION_ROWS || raw.arrow_bytes > B1_MAX_GENERATION_ARROW_BYTES {
        return Ok(raw);
    }
    post_tombstone_accounting(batch)
}

#[derive(Clone)]
pub(crate) struct ReplayGenerationState {
    pub(crate) generation: u64,
    pub(crate) replay_after_wal_entry_position: u64,
    pub(crate) accounting: GenerationAccounting,
    batch_store: Arc<BatchStore>,
}

impl std::fmt::Debug for ReplayGenerationState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplayGenerationState")
            .field("generation", &self.generation)
            .field(
                "replay_after_wal_entry_position",
                &self.replay_after_wal_entry_position,
            )
            .field("accounting", &self.accounting)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FlushedGenerationState {
    pub(crate) shard_manifest_version: u64,
    pub(crate) writer_epoch: u64,
    pub(crate) generation: u64,
    pub(crate) path: String,
    pub(crate) replay_after_wal_entry_position: u64,
}

#[derive(Debug, Clone)]
pub(crate) enum ActiveStreamDisposition {
    AdmitEmpty,
    FoldOnlyReplay(ReplayGenerationState),
    FoldOnlyFlushed(FlushedGenerationState),
}

fn merged_generation_for_bound_shard(
    details: &MemWalIndexDetails,
    shard_id: ShardId,
) -> Result<u64, MemWalWorkerError> {
    validate_bound_merge_progress(details, shard_id)?;
    let matches = details
        .merged_generations
        .iter()
        .filter(|merged| merged.shard_id == shard_id)
        .map(|merged| merged.generation)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [] => Ok(0),
        [generation] => Ok(*generation),
        _ => Err(MemWalWorkerError::state(format!(
            "multiple merged-generation cursors for bound shard {shard_id}: {matches:?}"
        ))),
    }
}

fn validate_manifest_identity(
    manifest: &ShardManifest,
    writer: &ShardWriter,
    epoch_floor: u64,
) -> Result<(), MemWalWorkerError> {
    if manifest.shard_id != writer.shard_id()
        || manifest.shard_spec_id != UNSHARDED_SPEC_ID
        || manifest.status != ShardStatus::Active
        || manifest.writer_epoch != writer.epoch()
        || writer.epoch() <= epoch_floor
    {
        return Err(MemWalWorkerError::state(format!(
            "claimed writer does not match active shard authority (writer shard={}, epoch={}; floor={epoch_floor}; manifest={manifest:?})",
            writer.shard_id(),
            writer.epoch(),
        )));
    }
    Ok(())
}

/// Reconstruct the only admitted/fold-only B1 state from public Lance state.
pub(crate) async fn classify_active_state(
    writer: &ShardWriter,
    details: &MemWalIndexDetails,
    epoch_floor: u64,
) -> Result<ActiveStreamDisposition, MemWalWorkerError> {
    let manifest = writer
        .manifest()
        .await
        .map_err(|error| MemWalWorkerError::lance("active manifest read", error))?
        .ok_or_else(|| MemWalWorkerError::state("claimed shard has no manifest"))?;
    validate_manifest_identity(&manifest, writer, epoch_floor)?;
    let in_memory = writer
        .in_memory_memtable_refs()
        .await
        .map_err(|error| MemWalWorkerError::lance("in-memory snapshot", error))?;
    classify_active_snapshot(&manifest, &in_memory, details)
}

fn classify_active_snapshot(
    manifest: &ShardManifest,
    in_memory: &InMemoryMemTables,
    details: &MemWalIndexDetails,
) -> Result<ActiveStreamDisposition, MemWalWorkerError> {
    if !in_memory.frozen.is_empty() {
        return Err(MemWalWorkerError::state(format!(
            "reopen observed {} frozen MemTables",
            in_memory.frozen.len()
        )));
    }
    if in_memory.active.generation != manifest.current_generation {
        return Err(MemWalWorkerError::state(format!(
            "active generation {} differs from authoritative current generation {}",
            in_memory.active.generation, manifest.current_generation
        )));
    }
    let accounting = account_store(&in_memory.active.batch_store)?;
    if !accounting.fits() {
        return Err(MemWalWorkerError::state(format!(
            "active generation exceeds B1 cap: {accounting:?}"
        )));
    }
    let (merged, unmerged) = exact_unmerged_generations(manifest, details)?;

    match (unmerged.as_slice(), accounting.rows) {
        ([], 0) => Ok(ActiveStreamDisposition::AdmitEmpty),
        // Lance WAL positions are 1-based, while cursor 0 means no MemTable
        // flush has stamped a replay watermark yet.  Reopening after an
        // invoked put may therefore legitimately rebuild a non-empty active
        // BatchStore from WAL position 1 while this field remains zero.  The
        // pointer/accounting bridge below proves that exact replay object and
        // stamps its complete prefix before sealing.
        ([], _) => Ok(ActiveStreamDisposition::FoldOnlyReplay(
            ReplayGenerationState {
                generation: manifest.current_generation,
                replay_after_wal_entry_position: manifest.replay_after_wal_entry_position,
                accounting,
                batch_store: Arc::clone(&in_memory.active.batch_store),
            },
        )),
        ([generation], 0)
            if generation.generation == merged.saturating_add(1)
                && manifest.current_generation == generation.generation.saturating_add(1) =>
        {
            Ok(ActiveStreamDisposition::FoldOnlyFlushed(
                FlushedGenerationState {
                    shard_manifest_version: manifest.version,
                    writer_epoch: manifest.writer_epoch,
                    generation: generation.generation,
                    path: generation.path.clone(),
                    replay_after_wal_entry_position: manifest.replay_after_wal_entry_position,
                },
            ))
        }
        ([generation], _) => Err(MemWalWorkerError::state(format!(
            "unmerged flushed generation {} overlaps non-empty active generation",
            generation.generation
        ))),
        (generations, _) => Err(MemWalWorkerError::state(format!(
            "expected at most one unmerged generation, observed {:?}",
            generations
                .iter()
                .map(|generation| generation.generation)
                .collect::<Vec<_>>()
        ))),
    }
}

/// Reconstruct the complete LWW projection of one already-flushed unmerged
/// generation before publishing a successor claim.
///
/// The successor's authenticated WAL suffix may contain only its fence
/// sentinel even though the immutable generation contains acknowledged PUTs
/// from the predecessor. Reading that exact generation is therefore required;
/// preserving the prior receipt would miss data flushed after the prior claim.
pub(crate) async fn scan_flushed_generation_projection(
    dataset: &Dataset,
    table_root_uri: &str,
    shard_id: ShardId,
    flushed: &FlushedGenerationState,
) -> Result<Vec<RecordBatch>, MemWalWorkerError> {
    let current_generation = flushed
        .generation
        .checked_add(1)
        .ok_or_else(|| MemWalWorkerError::state("flushed generation successor overflow"))?;
    let schema: ArrowSchema = dataset.schema().into();
    let snapshot = ShardSnapshot {
        shard_id,
        spec_id: UNSHARDED_SPEC_ID,
        current_generation,
        flushed_generations: vec![ScannerFlushedGeneration {
            generation: flushed.generation,
            path: flushed.path.clone(),
        }],
    };
    let mut scanner = LsmScanner::without_base_table(
        Arc::new(schema),
        table_root_uri.to_string(),
        vec![snapshot],
        vec!["id".to_string()],
    )
    .with_session(dataset.session());
    if let Some(store_params) = dataset.store_params() {
        scanner = scanner.with_store_params(store_params.clone());
    }
    scanner = scanner
        .limit(
            Some(
                i64::try_from(B1_MAX_GENERATION_ROWS + 1)
                    .expect("B1 row bound plus sentinel fits i64"),
            ),
            None,
        )
        .map_err(|error| MemWalWorkerError::lance("flushed projection row bound", error))?;
    let mut stream = scanner
        .try_into_stream()
        .await
        .map_err(|error| MemWalWorkerError::lance("flushed projection scan planning", error))?;
    let mut accounting = GenerationAccounting::default();
    let mut batches = Vec::new();
    while let Some(batch) = stream
        .try_next()
        .await
        .map_err(|error| MemWalWorkerError::lance("flushed projection scan", error))?
    {
        if batch.num_rows() == 0 {
            continue;
        }
        if batch
            .column_by_name(lance::dataset::mem_wal::TOMBSTONE)
            .is_some()
        {
            return Err(MemWalWorkerError::state(
                "fresh-only flushed projection unexpectedly exposed Lance's tombstone column",
            ));
        }
        let raw_accounting = accounting.checked_add(account_batch(&batch)?)?;
        if raw_accounting.rows > B1_MAX_GENERATION_ROWS {
            return Err(MemWalWorkerError::ResourceLimit {
                resource: "stream_claim_current_generation_rows",
                limit: B1_MAX_GENERATION_ROWS,
                actual: raw_accounting.rows,
            });
        }
        if raw_accounting.arrow_bytes > B1_MAX_GENERATION_ARROW_BYTES {
            return Err(MemWalWorkerError::ResourceLimit {
                resource: "stream_claim_current_generation_arrow_bytes",
                limit: B1_MAX_GENERATION_ARROW_BYTES,
                actual: raw_accounting.arrow_bytes,
            });
        }
        // LsmScanner returns the live LWW projection and deliberately omits
        // its internal tombstone. Claim hashing is defined over the exact
        // stored-generation schema, so restore the known-false column before
        // applying the generation bound and retaining the projection.
        let stored_schema = schema_with_tombstone(batch.schema().as_ref());
        let mut stored_columns = batch.columns().to_vec();
        stored_columns
            .push(Arc::new(BooleanArray::from(vec![false; batch.num_rows()])) as ArrayRef);
        let batch = RecordBatch::try_new(stored_schema, stored_columns)
            .map_err(|error| MemWalWorkerError::lance("projection tombstone restore", error))?;
        accounting = accounting.checked_add(account_batch(&batch)?)?;
        if accounting.rows > B1_MAX_GENERATION_ROWS {
            return Err(MemWalWorkerError::ResourceLimit {
                resource: "stream_claim_current_generation_rows",
                limit: B1_MAX_GENERATION_ROWS,
                actual: accounting.rows,
            });
        }
        if accounting.arrow_bytes > B1_MAX_GENERATION_ARROW_BYTES {
            return Err(MemWalWorkerError::ResourceLimit {
                resource: "stream_claim_current_generation_arrow_bytes",
                limit: B1_MAX_GENERATION_ARROW_BYTES,
                actual: accounting.arrow_bytes,
            });
        }
        if accounting.batches > B1_MAX_GENERATION_BATCHES {
            return Err(MemWalWorkerError::ResourceLimit {
                resource: "stream_claim_current_generation_batches",
                limit: B1_MAX_GENERATION_BATCHES as u64,
                actual: accounting.batches as u64,
            });
        }

        // Scanner output can be a small slice over a much larger backing
        // allocation. Copy every selected row into dense owned arrays so the
        // retained projection is bounded by the same logical accounting used
        // above, independent of allocator capacity or source-file layout.
        let row_count = u32::try_from(batch.num_rows())
            .map_err(|_| MemWalWorkerError::state("projection batch row count exceeds u32"))?;
        let indices = UInt32Array::from_iter_values(0..row_count);
        let columns = batch
            .columns()
            .iter()
            .map(|column| {
                take(column.as_ref(), &indices, None)
                    .map_err(|error| MemWalWorkerError::lance("projection densification", error))
            })
            .collect::<Result<Vec<_>, _>>()?;
        batches.push(
            RecordBatch::try_new(batch.schema(), columns)
                .map_err(|error| MemWalWorkerError::lance("projection batch rebuild", error))?,
        );
    }
    if accounting.rows == 0 {
        return Err(MemWalWorkerError::state(
            "flushed current-generation projection scan returned no rows",
        ));
    }
    Ok(batches)
}

/// Isolated RC.1 bridge: prove this is still the exact replay-derived active
/// BatchStore, then mark its complete contiguous prefix WAL-covered before
/// sealing.  This never writes a WAL entry.
pub(crate) async fn bridge_replayed_batch_store_watermark(
    writer: &ShardWriter,
    replay: &ReplayGenerationState,
) -> Result<(), MemWalWorkerError> {
    let manifest = writer
        .manifest()
        .await
        .map_err(|error| MemWalWorkerError::lance("replay bridge manifest read", error))?
        .ok_or_else(|| MemWalWorkerError::state("replay bridge found no shard manifest"))?;
    let in_memory = writer
        .in_memory_memtable_refs()
        .await
        .map_err(|error| MemWalWorkerError::lance("replay bridge snapshot", error))?;
    if !in_memory.frozen.is_empty()
        || in_memory.active.generation != replay.generation
        || manifest.current_generation != replay.generation
        || manifest.replay_after_wal_entry_position != replay.replay_after_wal_entry_position
        || !Arc::ptr_eq(&in_memory.active.batch_store, &replay.batch_store)
    {
        return Err(MemWalWorkerError::state(
            "replay bridge authority/snapshot changed before watermark update",
        ));
    }
    let observed = account_store(&in_memory.active.batch_store)?;
    if observed != replay.accounting || observed.batches == 0 {
        return Err(MemWalWorkerError::state(format!(
            "replay bridge prefix changed: expected {:?}, observed {observed:?}",
            replay.accounting
        )));
    }
    let last = observed.batches - 1;
    match in_memory.active.batch_store.max_flushed_batch_position() {
        None => in_memory
            .active
            .batch_store
            .set_max_flushed_batch_position(last),
        Some(existing) if existing == last => {}
        Some(existing) => {
            return Err(MemWalWorkerError::state(format!(
                "replay BatchStore has unexpected WAL watermark {existing}, expected {last}"
            )));
        }
    }
    Ok(())
}

/// A Lance shard writer whose epoch claim has succeeded but whose restart
/// topology has not necessarily been classified yet.
///
/// This wrapper is intentionally owning and non-cloneable.  Every failure
/// after the claim must transfer it to registry-managed retirement; dropping a
/// raw `ShardWriter` is not proof that its background tasks are quiescent.
pub(crate) struct ClaimedMemWalWorker {
    writer: Arc<ShardWriter>,
}

impl ClaimedMemWalWorker {
    pub(crate) fn new(writer: ShardWriter) -> Self {
        Self {
            writer: Arc::new(writer),
        }
    }

    pub(crate) fn writer(&self) -> &ShardWriter {
        &self.writer
    }

    pub(crate) async fn classify(
        self,
        dataset: &Dataset,
        table_root_uri: &str,
        details: &MemWalIndexDetails,
        epoch_floor: u64,
    ) -> Result<OpenedMemWalWorker, WorkerOpenFailure> {
        match classify_active_state(&self.writer, details, epoch_floor).await {
            Ok(disposition) => {
                let flushed_projection = match &disposition {
                    ActiveStreamDisposition::FoldOnlyFlushed(flushed) => {
                        match scan_flushed_generation_projection(
                            dataset,
                            table_root_uri,
                            self.writer().shard_id(),
                            flushed,
                        )
                        .await
                        {
                            Ok(batches) => Some(batches),
                            Err(error) => {
                                return Err(WorkerOpenFailure::claimed(error, self));
                            }
                        }
                    }
                    ActiveStreamDisposition::AdmitEmpty
                    | ActiveStreamDisposition::FoldOnlyReplay(_) => None,
                };
                Ok(OpenedMemWalWorker {
                    claimed: self,
                    disposition,
                    flushed_projection,
                })
            }
            Err(error) => Err(WorkerOpenFailure::claimed(error, self)),
        }
    }
}

pub(crate) struct OpenedMemWalWorker {
    claimed: ClaimedMemWalWorker,
    disposition: ActiveStreamDisposition,
    /// Dense, bounded LWW winners reconstructed from the exact immutable
    /// unmerged generation authenticated by `disposition`.
    ///
    /// This is consumed by terminal claim publication. Keeping it separate
    /// from `FlushedGenerationState` prevents the resident fold-only worker
    /// from retaining a redundant generation-sized projection afterward.
    flushed_projection: Option<Vec<RecordBatch>>,
}

/// Bounded physical input for the claim receipt's full current-generation LWW
/// commitment. A fence-only successor can authenticate an empty WAL suffix
/// while replaying rows from the prior epoch, so claim publication must not
/// derive this authority from the suffix alone.
pub(crate) enum CurrentGenerationProjectionSource {
    Empty,
    Replay(Vec<RecordBatch>),
    /// Defensive compatibility shape for an explicitly proven empty suffix.
    /// Normal and recovery opens recompute `Replay`; terminal publication must
    /// reject this shape if a newly authenticated suffix carries any row.
    PreservePrior,
}

impl OpenedMemWalWorker {
    pub(crate) async fn classify(
        writer: ShardWriter,
        dataset: &Dataset,
        table_root_uri: &str,
        details: &MemWalIndexDetails,
        epoch_floor: u64,
    ) -> Result<Self, WorkerOpenFailure> {
        ClaimedMemWalWorker::new(writer)
            .classify(dataset, table_root_uri, details, epoch_floor)
            .await
    }

    pub(crate) fn writer(&self) -> &ShardWriter {
        self.claimed.writer()
    }

    pub(crate) fn into_claimed(self) -> ClaimedMemWalWorker {
        self.claimed
    }

    pub(crate) fn current_generation_projection_source(
        &mut self,
    ) -> Result<CurrentGenerationProjectionSource, MemWalWorkerError> {
        match &self.disposition {
            ActiveStreamDisposition::AdmitEmpty => Ok(CurrentGenerationProjectionSource::Empty),
            ActiveStreamDisposition::FoldOnlyReplay(replay) => {
                Ok(CurrentGenerationProjectionSource::Replay(
                    replay
                        .batch_store
                        .iter()
                        .map(|stored| stored.data.clone())
                        .collect(),
                ))
            }
            ActiveStreamDisposition::FoldOnlyFlushed(_) => self
                .flushed_projection
                .take()
                .map(CurrentGenerationProjectionSource::Replay)
                .ok_or_else(|| {
                    MemWalWorkerError::state(
                        "flushed current-generation projection was already consumed",
                    )
                }),
        }
    }

    fn put_refusal(&self, table_key: &str) -> Option<OmniError> {
        match &self.disposition {
            ActiveStreamDisposition::AdmitEmpty => None,
            ActiveStreamDisposition::FoldOnlyReplay(replay) => Some(OmniError::FoldRequired {
                table_key: table_key.to_string(),
                rows: replay.accounting.rows,
                bytes: replay.accounting.arrow_bytes,
            }),
            ActiveStreamDisposition::FoldOnlyFlushed(_) => Some(OmniError::FoldRequired {
                table_key: table_key.to_string(),
                rows: 0,
                bytes: 0,
            }),
        }
    }

    fn fold_required_accounting(&self) -> Option<GenerationAccounting> {
        match &self.disposition {
            ActiveStreamDisposition::AdmitEmpty => None,
            ActiveStreamDisposition::FoldOnlyReplay(replay) => Some(replay.accounting),
            ActiveStreamDisposition::FoldOnlyFlushed(_) => Some(GenerationAccounting::default()),
        }
    }
}

pub(crate) struct WorkerOpenFailure {
    error: MemWalWorkerError,
    claimed: Option<ClaimedMemWalWorker>,
}

impl WorkerOpenFailure {
    pub(crate) fn unclaimed(error: MemWalWorkerError) -> Self {
        Self {
            error,
            claimed: None,
        }
    }

    pub(crate) fn claimed(error: MemWalWorkerError, claimed: ClaimedMemWalWorker) -> Self {
        Self {
            error,
            claimed: Some(claimed),
        }
    }

    pub(crate) fn into_parts(self) -> (MemWalWorkerError, Option<ClaimedMemWalWorker>) {
        (self.error, self.claimed)
    }
}

pub(crate) type WorkerOpenFuture =
    Pin<Box<dyn Future<Output = Result<OpenedMemWalWorker, WorkerOpenFailure>> + Send + 'static>>;
pub(crate) type WorkerOpener = Box<dyn FnOnce() -> WorkerOpenFuture + Send + 'static>;

pub(crate) enum PreparedPut {
    Warm {
        authority: CheckedStreamAuthority,
    },
    Cold {
        authority: CheckedStreamAuthority,
        opened: OpenedMemWalWorker,
    },
}

impl PreparedPut {
    pub(crate) fn warm(authority: CheckedStreamAuthority) -> Self {
        Self::Warm { authority }
    }

    pub(crate) fn cold(authority: CheckedStreamAuthority, opened: OpenedMemWalWorker) -> Self {
        Self::Cold { authority, opened }
    }
}

/// Failure after the shared admission lease has been acquired.
///
/// Warm failures transfer the lease so the existing cached writer can be
/// aborted before that lease is released.  Cold failures additionally carry
/// any successfully claimed Lance writer; `None` is valid only when the claim
/// itself never succeeded.
pub(crate) enum PreparedPutFailure {
    Warm {
        error: OmniError,
        authority: CheckedStreamAuthority,
    },
    Cold {
        error: OmniError,
        authority: CheckedStreamAuthority,
        worker: Option<FailedColdWorker>,
    },
}

pub(crate) enum FailedColdWorker {
    Claimed(ClaimedMemWalWorker),
    Opened(OpenedMemWalWorker),
}

impl PreparedPutFailure {
    pub(crate) fn warm(error: OmniError, authority: CheckedStreamAuthority) -> Self {
        Self::Warm { error, authority }
    }

    pub(crate) fn cold_unclaimed(error: OmniError, authority: CheckedStreamAuthority) -> Self {
        Self::Cold {
            error,
            authority,
            worker: None,
        }
    }

    pub(crate) fn cold_claimed(
        error: OmniError,
        authority: CheckedStreamAuthority,
        claimed: ClaimedMemWalWorker,
    ) -> Self {
        Self::Cold {
            error,
            authority,
            worker: Some(FailedColdWorker::Claimed(claimed)),
        }
    }

    pub(crate) fn cold_opened(
        error: OmniError,
        authority: CheckedStreamAuthority,
        opened: OpenedMemWalWorker,
    ) -> Self {
        Self::Cold {
            error,
            authority,
            worker: Some(FailedColdWorker::Opened(opened)),
        }
    }

    fn into_parts(self) -> (OmniError, CheckedStreamAuthority, Option<FailedColdWorker>) {
        match self {
            Self::Warm { error, authority } => (error, authority, None),
            Self::Cold {
                error,
                authority,
                worker,
            } => (error, authority, worker),
        }
    }
}

pub(crate) type PreparePutFuture =
    Pin<Box<dyn Future<Output = Result<PreparedPut, PreparedPutFailure>> + Send + 'static>>;
pub(crate) type PreparePut =
    Box<dyn FnOnce(Option<Arc<ShardWriter>>) -> PreparePutFuture + Send + 'static>;

/// A fresh authority check supplied by graph orchestration for the one
/// root-scoped idle task.  The factory receives the exact cached writer so it
/// can revalidate its epoch under the common shared admission lease.
pub(crate) type IdleAuthorityFuture =
    Pin<Box<dyn Future<Output = Result<CheckedStreamAuthority, IdleAuthorityFailure>> + Send>>;
pub(crate) type IdleAuthorityCheck =
    Arc<dyn Fn(Arc<ShardWriter>) -> IdleAuthorityFuture + Send + Sync + 'static>;

/// Infallible handoff from the detached physical owner to the root fold
/// supervisor. `true` means the current generation is already fold-only or a
/// new append hit its cap; `false` starts the normal max-staleness cadence.
pub(crate) type StreamFoldTrigger = Arc<dyn Fn(bool) + Send + Sync + 'static>;

/// Side-effect-free view of one resident worker while the caller owns that
/// table's exclusive stream-admission lease. `Missing` means the caller must
/// use the durable lifecycle/WAL probe; it must not create a worker merely to
/// decide whether a fold is ready.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResidentFoldReadiness {
    Missing,
    Idle,
    Ready,
    Busy,
}

pub(crate) struct IdleAuthorityFailure {
    error: OmniError,
    authority: CheckedStreamAuthority,
}

impl IdleAuthorityFailure {
    pub(crate) fn new(error: OmniError, authority: CheckedStreamAuthority) -> Self {
        Self { error, authority }
    }

    fn into_parts(self) -> (OmniError, CheckedStreamAuthority) {
        (self.error, self.authority)
    }
}

#[derive(Debug)]
enum WorkerMode {
    Admit(GenerationAccounting),
    FoldReplay(ReplayGenerationState),
    FoldFlushed(FlushedGenerationState),
    Retiring,
}

#[derive(Clone, Copy)]
enum EmptyCutPolicy {
    Reject,
    ReturnFence,
}

struct MemWalWorker {
    key: StreamWorkerKey,
    table_key: String,
    writer: Arc<ShardWriter>,
    mode: tokio::sync::Mutex<WorkerMode>,
    /// Warm watcher-confirmed projection for the one live generation.  Cold
    /// replay/fold-only workers always start empty and never reconstruct it.
    confirmed_token_overlay: Mutex<ConfirmedStreamTokenOverlay>,
    usage: Mutex<WorkerUsage>,
    last_used: Mutex<std::time::Instant>,
    idle_task_armed: AtomicBool,
    /// Wakes the one idle owner as soon as this worker leaves `Active`.
    ///
    /// The idle owner retains its graph-scoped authority factory, so letting
    /// it sleep until the normal idle deadline would also retain the checked
    /// runtime after quiescence has retired the worker.
    idle_retirement: tokio::sync::Notify,
}

#[derive(Debug, Clone, Copy, Default)]
struct WorkerUsage {
    resident: bool,
    accounting: GenerationAccounting,
    pending_generations: usize,
    /// Whether `accounting` and `pending_generations` are represented in the
    /// root registry. The resident-writer reservation is acquired separately
    /// before a cold claim and is represented whenever `resident` is true.
    generation_registered: bool,
}

impl MemWalWorker {
    fn from_opened(key: StreamWorkerKey, table_key: String, opened: OpenedMemWalWorker) -> Self {
        let mode = match opened.disposition {
            ActiveStreamDisposition::AdmitEmpty => {
                WorkerMode::Admit(GenerationAccounting::default())
            }
            ActiveStreamDisposition::FoldOnlyReplay(replay) => WorkerMode::FoldReplay(replay),
            ActiveStreamDisposition::FoldOnlyFlushed(flushed) => WorkerMode::FoldFlushed(flushed),
        };
        let usage = match &mode {
            WorkerMode::Admit(_) => WorkerUsage {
                resident: true,
                ..WorkerUsage::default()
            },
            WorkerMode::FoldReplay(replay) => WorkerUsage {
                resident: true,
                accounting: replay.accounting,
                pending_generations: 1,
                generation_registered: false,
            },
            WorkerMode::FoldFlushed(_) => WorkerUsage {
                resident: true,
                pending_generations: 1,
                ..WorkerUsage::default()
            },
            WorkerMode::Retiring => unreachable!("opened worker cannot start retired"),
        };
        Self {
            key,
            table_key,
            writer: opened.claimed.writer,
            mode: tokio::sync::Mutex::new(mode),
            confirmed_token_overlay: Mutex::new(BTreeMap::new()),
            usage: Mutex::new(usage),
            last_used: Mutex::new(std::time::Instant::now()),
            idle_task_armed: AtomicBool::new(false),
            idle_retirement: tokio::sync::Notify::new(),
        }
    }

    /// Install a successfully claimed but unclassified writer solely so the
    /// registry can retain its admission lease through one owned abort.
    fn retiring_from_claimed(
        key: StreamWorkerKey,
        table_key: String,
        claimed: ClaimedMemWalWorker,
    ) -> Self {
        Self {
            key,
            table_key,
            writer: claimed.writer,
            mode: tokio::sync::Mutex::new(WorkerMode::Retiring),
            confirmed_token_overlay: Mutex::new(BTreeMap::new()),
            usage: Mutex::new(WorkerUsage {
                resident: true,
                accounting: GenerationAccounting {
                    rows: 0,
                    arrow_bytes: B1_MAX_GENERATION_ARROW_BYTES,
                    batches: 0,
                },
                pending_generations: 1,
                generation_registered: false,
            }),
            last_used: Mutex::new(std::time::Instant::now()),
            idle_task_armed: AtomicBool::new(false),
            idle_retirement: tokio::sync::Notify::new(),
        }
    }

    fn ack_unknown(&self, ordinals: CallerOrdinalRange, reason: impl Into<String>) -> OmniError {
        OmniError::AckUnknown {
            stable_table_id: self.key.identity.stable_table_id,
            table_incarnation_id: self.key.identity.table_incarnation_id,
            enrollment_id: self.key.enrollment_id.to_string(),
            shard_id: self.key.shard_id.to_string(),
            writer_epoch: self.writer.epoch(),
            caller_ordinal_start: ordinals.start,
            caller_ordinal_end: ordinals.end,
            admission_attempt_id: None,
            logical_write_ids: Vec::new(),
            unconfirmed_candidate_token: None,
            reason: reason.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AbortOutcome {
    error: Option<String>,
}

#[derive(Clone)]
pub(crate) struct RetirementHandle {
    receiver: watch::Receiver<Option<AbortOutcome>>,
}

impl RetirementHandle {
    pub(crate) async fn wait(mut self) -> Result<(), MemWalWorkerError> {
        loop {
            if let Some(outcome) = self.receiver.borrow().clone() {
                return match outcome.error {
                    None => Ok(()),
                    Some(reason) => Err(MemWalWorkerError::state(format!(
                        "background abort failed: {reason}"
                    ))),
                };
            }
            self.receiver
                .changed()
                .await
                .map_err(|_| MemWalWorkerError::state("background abort outcome channel closed"))?;
        }
    }
}

enum RegistrySlotState {
    Vacant,
    Opening(Arc<tokio::sync::Notify>),
    Active(Arc<MemWalWorker>),
    Retiring(RetirementHandle),
}

struct RegistrySlot {
    state: tokio::sync::Mutex<RegistrySlotState>,
    /// Serializes one caller batch from pre-detach accounting until its exact
    /// charge transfers into this worker (or is refused).
    input_queue: Arc<tokio::sync::Mutex<()>>,
}

impl Default for RegistrySlot {
    fn default() -> Self {
        Self {
            state: tokio::sync::Mutex::new(RegistrySlotState::Vacant),
            input_queue: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
}

/// One weakly root-scoped registry shared by every independently-opened graph
/// handle in this process.
pub(crate) struct MemWalWorkerRegistry {
    slots: Mutex<HashMap<StreamWorkerKey, Arc<RegistrySlot>>>,
    limits: B1WorkerLimits,
    usage: Mutex<RegistryUsage>,
}

#[derive(Debug, Default)]
struct RegistryUsage {
    resident_writers_root: usize,
    resident_writers_by_table: HashMap<TableIdentity, usize>,
    reserved_arrow_bytes: u64,
    /// Conservative scratch reservation held before a B2 row can materialize
    /// external blobs or allocate its canonical payload.
    b2_preprocessing_bytes: u64,
    /// Exact resident plus queued generation accounting by physical worker
    /// key. This is the decomposition of `reserved_arrow_bytes`, not a second
    /// lifecycle source of truth; it exists so same-key cap crossings retain
    /// the typed `FoldRequired` contract under overlap.
    reserved_by_key: HashMap<StreamWorkerKey, GenerationAccounting>,
    /// Derived refusal marker for an active replay/flushed generation. It is
    /// installed before the opener releases the same-key input queue, so new
    /// callers cannot add more Arrow charge while already-admitted callers
    /// drain to `FoldRequired`.
    fold_required_by_key: HashMap<StreamWorkerKey, GenerationAccounting>,
    inflight_calls: usize,
    pending_generations: usize,
}

fn promote_fold_usage_counters(
    registry: &mut RegistryUsage,
    worker: &mut WorkerUsage,
    key: StreamWorkerKey,
    limits: &B1WorkerLimits,
) -> Result<(), MemWalWorkerError> {
    if !worker.resident || !worker.generation_registered || worker.pending_generations != 1 {
        return Err(MemWalWorkerError::state(format!(
            "fold reservation requires one resident pending generation, observed {worker:?}"
        )));
    }
    if worker.accounting.arrow_bytes > B1_MAX_GENERATION_ARROW_BYTES {
        return Err(MemWalWorkerError::state(format!(
            "resident generation reservation {} exceeds B1 cap {}",
            worker.accounting.arrow_bytes, B1_MAX_GENERATION_ARROW_BYTES
        )));
    }
    if registry.reserved_arrow_bytes < worker.accounting.arrow_bytes
        || registry.pending_generations < worker.pending_generations
    {
        return Err(MemWalWorkerError::state(
            "worker fold reservation is not represented in aggregate counters",
        ));
    }
    let additional = B1_MAX_GENERATION_ARROW_BYTES - worker.accounting.arrow_bytes;
    let aggregate = registry
        .reserved_arrow_bytes
        .checked_add(additional)
        .ok_or_else(|| MemWalWorkerError::state("aggregate fold reservation overflow"))?;
    if aggregate > limits.max_reserved_arrow_bytes {
        return Err(MemWalWorkerError::ResourceLimit {
            resource: "stream_reserved_arrow_bytes",
            limit: limits.max_reserved_arrow_bytes,
            actual: aggregate,
        });
    }
    let current_for_key = registry
        .reserved_by_key
        .get(&key)
        .copied()
        .unwrap_or_default();
    if current_for_key.rows < worker.accounting.rows
        || current_for_key.arrow_bytes < worker.accounting.arrow_bytes
        || current_for_key.batches < worker.accounting.batches
    {
        return Err(MemWalWorkerError::state(
            "worker fold reservation is not represented in per-key counters",
        ));
    }
    let promoted_for_key = current_for_key.checked_add(GenerationAccounting {
        rows: 0,
        arrow_bytes: additional,
        batches: 0,
    })?;
    if !promoted_for_key.fits() {
        return Err(MemWalWorkerError::ResourceLimit {
            resource: "stream_reserved_arrow_bytes",
            limit: B1_MAX_GENERATION_ARROW_BYTES,
            actual: promoted_for_key.arrow_bytes,
        });
    }
    registry.reserved_arrow_bytes = aggregate;
    registry.reserved_by_key.insert(key, promoted_for_key);
    worker.accounting.arrow_bytes = B1_MAX_GENERATION_ARROW_BYTES;
    Ok(())
}

struct InFlightPermit {
    registry: Arc<MemWalWorkerRegistry>,
}

/// Root-scoped ownership for B2 preprocessing scratch. It is acquired before
/// blob materialization or canonical encoding and retains the ordinary
/// inflight slot until that ownership transfers into [`QueuedBatchPermit`].
pub(crate) struct B2PreprocessingPermit {
    registry: Arc<MemWalWorkerRegistry>,
    bytes: u64,
    inflight: Option<InFlightPermit>,
}

impl B2PreprocessingPermit {
    fn take_inflight(&mut self) -> InFlightPermit {
        self.inflight
            .take()
            .expect("B2 preprocessing inflight permit transfers exactly once")
    }
}

impl Drop for B2PreprocessingPermit {
    fn drop(&mut self) {
        let mut usage = self
            .registry
            .usage
            .lock()
            .expect("MemWAL registry usage poisoned");
        usage.b2_preprocessing_bytes = usage
            .b2_preprocessing_bytes
            .checked_sub(self.bytes)
            .expect("B2 preprocessing reservation accounting underflow");
    }
}

impl Drop for InFlightPermit {
    fn drop(&mut self) {
        let mut usage = self
            .registry
            .usage
            .lock()
            .expect("MemWAL registry usage poisoned");
        usage.inflight_calls = usage.inflight_calls.saturating_sub(1);
    }
}

/// Own the exact post-tombstone Arrow charge from synchronous registry entry
/// until it is either rejected or transferred into the resident generation.
/// This closes the otherwise-unaccounted queue between caller admission and a
/// serialized worker's `put_no_wait` invocation.
pub(crate) struct QueuedBatchPermit {
    registry: Arc<MemWalWorkerRegistry>,
    key: StreamWorkerKey,
    charge: GenerationAccounting,
    transferred: bool,
    _input_queue: Option<tokio::sync::OwnedMutexGuard<()>>,
    inflight: Option<InFlightPermit>,
}

impl QueuedBatchPermit {
    fn charge(&self) -> GenerationAccounting {
        self.charge
    }

    fn take_inflight(&mut self) -> InFlightPermit {
        self.inflight
            .take()
            .expect("queued batch inflight permit transfers exactly once")
    }

    /// Replace the preliminary logical-batch charge with the exact stored
    /// batch after trusted stream metadata has been classified and appended.
    ///
    /// The preliminary charge is acquired before shared admission and the
    /// same-key queue, so caller-owned row buffers are never unaccounted. This
    /// adjustment is immediate/non-waiting and happens while that queue
    /// position is held, before worker-mode inspection or any Lance call.
    pub(crate) fn reprice_for_exact_batch(
        &mut self,
        table_key: &str,
        batch: &RecordBatch,
    ) -> OmniResult<()> {
        if self.transferred {
            return Err(OmniError::manifest_internal(
                "cannot reprice a stream batch after its charge transferred to a worker",
            ));
        }
        let exact =
            b1_input_accounting(batch).map_err(|error| OmniError::Lance(error.to_string()))?;
        let mut usage = self
            .registry
            .usage
            .lock()
            .expect("MemWAL registry usage poisoned");
        let current = usage
            .reserved_by_key
            .get(&self.key)
            .copied()
            .ok_or_else(|| {
                OmniError::manifest_internal(
                    "queued stream charge disappeared before exact repricing",
                )
            })?;
        let without_preliminary = current
            .checked_sub(self.charge)
            .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
        let projected = without_preliminary
            .checked_add(exact)
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        if !projected.fits() {
            return Err(OmniError::FoldRequired {
                table_key: table_key.to_string(),
                rows: projected.rows,
                bytes: projected.arrow_bytes,
            });
        }
        let root_without_preliminary = usage
            .reserved_arrow_bytes
            .checked_sub(self.charge.arrow_bytes)
            .ok_or_else(|| {
                OmniError::manifest_internal("root stream charge underflow during exact repricing")
            })?;
        let root_projected = root_without_preliminary
            .checked_add(exact.arrow_bytes)
            .ok_or_else(|| {
                MemWalWorkerRegistry::resource_error(
                    "stream_reserved_arrow_bytes",
                    self.registry.limits.max_reserved_arrow_bytes,
                    u64::MAX,
                )
            })?;
        if root_projected > self.registry.limits.max_reserved_arrow_bytes {
            return Err(MemWalWorkerRegistry::resource_error(
                "stream_reserved_arrow_bytes",
                self.registry.limits.max_reserved_arrow_bytes,
                root_projected,
            ));
        }
        usage.reserved_arrow_bytes = root_projected;
        usage.reserved_by_key.insert(self.key, projected);
        self.charge = exact;
        Ok(())
    }

    fn transfer_to_worker(
        mut self,
        worker: &MemWalWorker,
        current: GenerationAccounting,
    ) -> OmniResult<()> {
        self.registry
            .transfer_batch_usage(worker, current, self.charge)?;
        self.transferred = true;
        Ok(())
    }
}

impl Drop for QueuedBatchPermit {
    fn drop(&mut self) {
        if !self.transferred {
            self.registry.release_queued_charge(self.key, self.charge);
        }
    }
}

/// Conservative full-generation reservation installed before a cold fold can
/// invoke Lance's writer opener. Until it transfers into a concrete worker it
/// owns resident, pending, byte, and same-key refusal accounting itself.
struct OpeningFoldUsagePermit {
    registry: Arc<MemWalWorkerRegistry>,
    key: StreamWorkerKey,
    usage: WorkerUsage,
    transferred: bool,
}

impl OpeningFoldUsagePermit {
    fn transfer_to_worker(
        &mut self,
        worker: &MemWalWorker,
        fold_required: GenerationAccounting,
    ) -> Result<(), MemWalWorkerError> {
        self.registry
            .transfer_fold_opening_usage(worker, fold_required)?;
        self.transferred = true;
        Ok(())
    }

    fn into_fold_usage(mut self, inflight: InFlightPermit) -> FoldUsagePermit {
        self.transferred = true;
        FoldUsagePermit {
            registry: Arc::clone(&self.registry),
            key: self.key,
            usage: self.usage,
            _inflight: inflight,
        }
    }
}

impl Drop for OpeningFoldUsagePermit {
    fn drop(&mut self) {
        if !self.transferred {
            self.registry.release_usage(self.key, self.usage);
        }
    }
}

/// Successful seal transfers both the full conservative generation charge and
/// the operation's inflight permit here. The cut owns this through fresh scan,
/// staging, recovery, table commit, and graph publication.
struct FoldUsagePermit {
    registry: Arc<MemWalWorkerRegistry>,
    key: StreamWorkerKey,
    usage: WorkerUsage,
    _inflight: InFlightPermit,
}

impl Drop for FoldUsagePermit {
    fn drop(&mut self) {
        self.registry.release_usage(self.key, self.usage);
    }
}

impl MemWalWorkerRegistry {
    pub(crate) fn for_root(
        root_identity: &str,
        limits: B1WorkerLimits,
    ) -> Result<Arc<Self>, MemWalWorkerError> {
        limits.validate()?;
        static REGISTRY: OnceLock<Mutex<HashMap<String, Weak<MemWalWorkerRegistry>>>> =
            OnceLock::new();
        let registry = REGISTRY.get_or_init(|| Mutex::new(HashMap::new()));
        let mut roots = registry.lock().expect("MemWAL root registry poisoned");
        if let Some(existing) = roots.get(root_identity).and_then(Weak::upgrade) {
            if existing.limits != limits {
                return Err(MemWalWorkerError::config(format!(
                    "root {root_identity:?} already has different B1 worker limits: existing={:?}, requested={limits:?}",
                    existing.limits
                )));
            }
            return Ok(existing);
        }
        roots.retain(|_, registry| registry.strong_count() > 0);
        let created = Arc::new(Self {
            slots: Mutex::new(HashMap::new()),
            limits,
            usage: Mutex::new(RegistryUsage::default()),
        });
        roots.insert(root_identity.to_string(), Arc::downgrade(&created));
        Ok(created)
    }

    fn slot(&self, key: StreamWorkerKey) -> Arc<RegistrySlot> {
        let mut slots = self.slots.lock().expect("MemWAL worker slots poisoned");
        Arc::clone(slots.entry(key).or_default())
    }

    fn resource_error(resource: &'static str, limit: u64, actual: u64) -> OmniError {
        OmniError::ResourceLimitExceeded {
            resource: resource.to_string(),
            limit,
            actual,
        }
    }

    fn worker_error_to_omni(error: MemWalWorkerError) -> OmniError {
        match error {
            MemWalWorkerError::ResourceLimit {
                resource,
                limit,
                actual,
            } => Self::resource_error(resource, limit, actual),
            other => OmniError::Lance(other.to_string()),
        }
    }

    fn reserve_inflight_core(self: &Arc<Self>) -> Result<InFlightPermit, MemWalWorkerError> {
        let mut usage = self.usage.lock().expect("MemWAL registry usage poisoned");
        let actual = usage.inflight_calls.saturating_add(1);
        if actual > self.limits.max_inflight_calls {
            return Err(MemWalWorkerError::ResourceLimit {
                resource: "stream_inflight_calls",
                limit: self.limits.max_inflight_calls as u64,
                actual: actual as u64,
            });
        }
        usage.inflight_calls = actual;
        Ok(InFlightPermit {
            registry: Arc::clone(self),
        })
    }

    /// Reserve the complete worst-case scratch envelope before B2 can resolve
    /// blobs or allocate canonical payload bytes. The ordinary inflight slot
    /// moves from this permit into the queued/worker corridor; scratch remains
    /// reserved until canonical hashing is complete.
    pub(crate) fn reserve_b2_preprocessing(self: &Arc<Self>) -> OmniResult<B2PreprocessingPermit> {
        let inflight = self
            .reserve_inflight_core()
            .map_err(Self::worker_error_to_omni)?;
        let bytes = B2_PREPROCESSING_RESERVATION_BYTES;
        let mut usage = self.usage.lock().expect("MemWAL registry usage poisoned");
        let actual = usage
            .b2_preprocessing_bytes
            .checked_add(bytes)
            .ok_or_else(|| {
                Self::resource_error(
                    "stream_b2_preprocessing_bytes",
                    self.limits.max_b2_preprocessing_bytes,
                    u64::MAX,
                )
            })?;
        if actual > self.limits.max_b2_preprocessing_bytes {
            return Err(Self::resource_error(
                "stream_b2_preprocessing_bytes",
                self.limits.max_b2_preprocessing_bytes,
                actual,
            ));
        }
        usage.b2_preprocessing_bytes = actual;
        drop(usage);
        Ok(B2PreprocessingPermit {
            registry: Arc::clone(self),
            bytes,
            inflight: Some(inflight),
        })
    }

    fn ensure_input_projection_fits(
        table_key: &str,
        mode: &WorkerMode,
        charge: GenerationAccounting,
    ) -> OmniResult<()> {
        match mode {
            WorkerMode::Admit(current) => {
                let projected = current
                    .checked_add(charge)
                    .map_err(|error| OmniError::Lance(error.to_string()))?;
                if !projected.fits() {
                    return Err(OmniError::FoldRequired {
                        table_key: table_key.to_string(),
                        rows: projected.rows,
                        bytes: projected.arrow_bytes,
                    });
                }
            }
            WorkerMode::FoldReplay(replay) => {
                return Err(OmniError::FoldRequired {
                    table_key: table_key.to_string(),
                    rows: replay.accounting.rows,
                    bytes: replay.accounting.arrow_bytes,
                });
            }
            WorkerMode::FoldFlushed(_) => {
                return Err(OmniError::FoldRequired {
                    table_key: table_key.to_string(),
                    rows: 0,
                    bytes: 0,
                });
            }
            WorkerMode::Retiring => {}
        }
        Ok(())
    }

    async fn active_worker(slot: &RegistrySlot) -> Option<Arc<MemWalWorker>> {
        let state = slot.state.lock().await;
        match &*state {
            RegistrySlotState::Active(worker) => Some(Arc::clone(worker)),
            _ => None,
        }
    }

    pub(crate) async fn resident_fold_readiness(
        &self,
        key: StreamWorkerKey,
    ) -> ResidentFoldReadiness {
        let slot = self
            .slots
            .lock()
            .expect("MemWAL worker registry poisoned")
            .get(&key)
            .cloned();
        let Some(slot) = slot else {
            return ResidentFoldReadiness::Missing;
        };
        let worker = {
            let state = slot.state.lock().await;
            match &*state {
                RegistrySlotState::Vacant => return ResidentFoldReadiness::Missing,
                RegistrySlotState::Opening(_) | RegistrySlotState::Retiring(_) => {
                    return ResidentFoldReadiness::Busy;
                }
                RegistrySlotState::Active(worker) => Arc::clone(worker),
            }
        };
        let mode = worker.mode.lock().await;
        match &*mode {
            WorkerMode::Admit(accounting) if *accounting == GenerationAccounting::default() => {
                ResidentFoldReadiness::Idle
            }
            WorkerMode::Admit(_) | WorkerMode::FoldReplay(_) | WorkerMode::FoldFlushed(_) => {
                ResidentFoldReadiness::Ready
            }
            WorkerMode::Retiring => ResidentFoldReadiness::Busy,
        }
    }

    fn reserve_queued_charge(
        self: &Arc<Self>,
        key: StreamWorkerKey,
        table_key: &str,
        charge: GenerationAccounting,
        inflight: InFlightPermit,
        input_queue: Option<tokio::sync::OwnedMutexGuard<()>>,
    ) -> OmniResult<QueuedBatchPermit> {
        let mut usage = self.usage.lock().expect("MemWAL registry usage poisoned");
        if let Some(accounting) = usage.fold_required_by_key.get(&key).copied() {
            return Err(OmniError::FoldRequired {
                table_key: table_key.to_string(),
                rows: accounting.rows,
                bytes: accounting.arrow_bytes,
            });
        }
        let projected = usage
            .reserved_by_key
            .get(&key)
            .copied()
            .unwrap_or_default()
            .checked_add(charge)
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        if !projected.fits() {
            return Err(OmniError::FoldRequired {
                table_key: table_key.to_string(),
                rows: projected.rows,
                bytes: projected.arrow_bytes,
            });
        }
        let actual = usage
            .reserved_arrow_bytes
            .checked_add(charge.arrow_bytes)
            .ok_or_else(|| {
                Self::resource_error(
                    "stream_reserved_arrow_bytes",
                    self.limits.max_reserved_arrow_bytes,
                    u64::MAX,
                )
            })?;
        if actual > self.limits.max_reserved_arrow_bytes {
            return Err(Self::resource_error(
                "stream_reserved_arrow_bytes",
                self.limits.max_reserved_arrow_bytes,
                actual,
            ));
        }
        usage.reserved_arrow_bytes = actual;
        usage.reserved_by_key.insert(key, projected);
        Ok(QueuedBatchPermit {
            registry: Arc::clone(self),
            key,
            charge,
            transferred: false,
            _input_queue: input_queue,
            inflight: Some(inflight),
        })
    }

    /// Bound every caller before it can acquire shared fold admission or wait
    /// on the same-key queue. Every put follows the one lock order
    /// charge -> shared admission -> input queue -> worker mode. A queue-first
    /// fast path is forbidden: with Tokio's fair admission lock it can form a
    /// three-way owner/fold/waiter cycle.
    pub(crate) async fn reserve_put_input<F, Fut>(
        self: &Arc<Self>,
        key: StreamWorkerKey,
        table_key: &str,
        batch: &RecordBatch,
        acquire_authority: F,
    ) -> OmniResult<(QueuedBatchPermit, CheckedStreamAuthority)>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = CheckedStreamAuthority> + Send,
    {
        let inflight = self
            .reserve_inflight_core()
            .map_err(Self::worker_error_to_omni)?;
        self.reserve_put_input_with_inflight(key, table_key, batch, acquire_authority, inflight)
            .await
    }

    /// B2 sibling of [`Self::reserve_put_input`]. Preprocessing already owns
    /// the inflight slot, so this transfers it without opening an unbounded
    /// interval between scratch allocation and root accounting.
    pub(crate) async fn reserve_b2_put_input<F, Fut>(
        self: &Arc<Self>,
        key: StreamWorkerKey,
        table_key: &str,
        batch: &RecordBatch,
        preprocessing: &mut B2PreprocessingPermit,
        acquire_authority: F,
    ) -> OmniResult<(QueuedBatchPermit, CheckedStreamAuthority)>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = CheckedStreamAuthority> + Send,
    {
        if !Arc::ptr_eq(self, &preprocessing.registry) {
            return Err(OmniError::manifest_internal(
                "B2 preprocessing permit belongs to another root registry",
            ));
        }
        let inflight = preprocessing.take_inflight();
        self.reserve_put_input_with_inflight(key, table_key, batch, acquire_authority, inflight)
            .await
    }

    async fn reserve_put_input_with_inflight<F, Fut>(
        self: &Arc<Self>,
        key: StreamWorkerKey,
        table_key: &str,
        batch: &RecordBatch,
        acquire_authority: F,
        inflight: InFlightPermit,
    ) -> OmniResult<(QueuedBatchPermit, CheckedStreamAuthority)>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = CheckedStreamAuthority> + Send,
    {
        let charge =
            b1_input_accounting(batch).map_err(|error| OmniError::Lance(error.to_string()))?;
        if !charge.fits() {
            return Err(OmniError::FoldRequired {
                table_key: table_key.to_string(),
                rows: charge.rows,
                bytes: charge.arrow_bytes,
            });
        }
        let slot = self.slot(key);
        let mut queued = self.reserve_queued_charge(key, table_key, charge, inflight, None)?;
        let authority = acquire_authority().await;
        crate::failpoints::maybe_fail(
            crate::failpoints::names::STREAM_B1_AFTER_SHARED_BEFORE_QUEUE_WAIT,
        )?;
        queued._input_queue = Some(Arc::clone(&slot.input_queue).lock_owned().await);
        if let Some(worker) = Self::active_worker(&slot).await {
            let mode = worker.mode.lock().await;
            Self::ensure_input_projection_fits(table_key, &mode, charge)?;
        }
        Ok((queued, authority))
    }

    /// Read one watcher-confirmed token while the caller owns this key's input-
    /// queue position. A vacant worker means an empty live
    /// generation.  Replayed/flushed state is fold-only and therefore cannot
    /// be treated as a reconstructed sequencing overlay.
    pub(crate) async fn confirmed_token_for_key(
        self: &Arc<Self>,
        queued: &QueuedBatchPermit,
        table_key: &str,
        logical_id: &str,
    ) -> OmniResult<Option<ConfirmedStreamTokenOverlayRow>> {
        if !Arc::ptr_eq(self, &queued.registry) {
            return Err(OmniError::manifest_internal(
                "stream token overlay permit belongs to another root registry",
            ));
        }
        let slot = self.slot(queued.key);
        let worker = {
            let state = slot.state.lock().await;
            match &*state {
                RegistrySlotState::Vacant => return Ok(None),
                RegistrySlotState::Active(worker) => Arc::clone(worker),
                RegistrySlotState::Opening(_) => {
                    return Err(OmniError::manifest_internal(
                        "stream token overlay observed an opener despite owning the same-key input queue",
                    ));
                }
                RegistrySlotState::Retiring(_) => {
                    return Err(OmniError::Lance(
                        MemWalWorkerError::Retiring {
                            key: queued.key,
                            reason: "original abort completion has not settled".to_string(),
                        }
                        .to_string(),
                    ));
                }
            }
        };
        let mode = worker.mode.lock().await;
        match &*mode {
            WorkerMode::Admit(_) => {}
            WorkerMode::FoldReplay(replay) => {
                return Err(OmniError::FoldRequired {
                    table_key: table_key.to_string(),
                    rows: replay.accounting.rows,
                    bytes: replay.accounting.arrow_bytes,
                });
            }
            WorkerMode::FoldFlushed(_) => {
                return Err(OmniError::FoldRequired {
                    table_key: table_key.to_string(),
                    rows: 0,
                    bytes: 0,
                });
            }
            WorkerMode::Retiring => {
                return Err(OmniError::Lance(
                    MemWalWorkerError::Retiring {
                        key: queued.key,
                        reason: "token overlay is unavailable while the worker retires".to_string(),
                    }
                    .to_string(),
                ));
            }
        }
        let current = worker
            .confirmed_token_overlay
            .lock()
            .expect("MemWAL token overlay poisoned")
            .get(logical_id)
            .cloned();
        Ok(current)
    }

    /// Return the exact current-generation token winners after applying one
    /// queued update set. The caller owns the same-key input queue, so this
    /// projection cannot race a watcher-confirmed overlay installation. It is
    /// used to enforce recovery-v12/token-table byte bounds before Lance is
    /// invoked and an acknowledgement can become possible.
    pub(crate) async fn projected_token_authority_rows(
        self: &Arc<Self>,
        queued: &QueuedBatchPermit,
        table_key: &str,
        updates: &ConfirmedStreamTokenOverlay,
    ) -> OmniResult<Vec<StreamTokenAuthorityRow>> {
        if !Arc::ptr_eq(self, &queued.registry) {
            return Err(OmniError::manifest_internal(
                "stream token projection permit belongs to another root registry",
            ));
        }
        for (logical_id, update) in updates {
            update.validate(queued.key.identity, logical_id)?;
        }
        let slot = self.slot(queued.key);
        let worker = {
            let state = slot.state.lock().await;
            match &*state {
                RegistrySlotState::Vacant => None,
                RegistrySlotState::Active(worker) => Some(Arc::clone(worker)),
                RegistrySlotState::Opening(_) => {
                    return Err(OmniError::manifest_internal(
                        "stream token projection observed an opener despite owning the same-key input queue",
                    ));
                }
                RegistrySlotState::Retiring(_) => {
                    return Err(OmniError::Lance(
                        MemWalWorkerError::Retiring {
                            key: queued.key,
                            reason: "original abort completion has not settled".to_string(),
                        }
                        .to_string(),
                    ));
                }
            }
        };
        let mut projected = ConfirmedStreamTokenOverlay::new();
        if let Some(worker) = worker {
            let mode = worker.mode.lock().await;
            match &*mode {
                WorkerMode::Admit(_) => {}
                WorkerMode::FoldReplay(replay) => {
                    return Err(OmniError::FoldRequired {
                        table_key: table_key.to_string(),
                        rows: replay.accounting.rows,
                        bytes: replay.accounting.arrow_bytes,
                    });
                }
                WorkerMode::FoldFlushed(_) => {
                    return Err(OmniError::FoldRequired {
                        table_key: table_key.to_string(),
                        rows: 0,
                        bytes: 0,
                    });
                }
                WorkerMode::Retiring => {
                    return Err(OmniError::Lance(format!(
                        "MemWAL worker {} is retiring",
                        queued.key
                    )));
                }
            }
            projected = worker
                .confirmed_token_overlay
                .lock()
                .expect("MemWAL token overlay poisoned")
                .clone();
        }
        projected.extend(updates.clone());
        Ok(projected.into_values().map(|row| row.authority).collect())
    }

    fn release_queued_charge(&self, key: StreamWorkerKey, charge: GenerationAccounting) {
        let mut usage = self.usage.lock().expect("MemWAL registry usage poisoned");
        let current = usage.reserved_by_key.get(&key).copied().unwrap_or_default();
        let remaining = match current.checked_sub(charge) {
            Ok(remaining) => remaining,
            Err(error) => {
                debug_assert!(false, "queued per-key accounting release failed: {error}");
                return;
            }
        };
        if usage.reserved_arrow_bytes < charge.arrow_bytes {
            debug_assert!(false, "queued aggregate Arrow accounting underflow");
            return;
        }
        usage.reserved_arrow_bytes -= charge.arrow_bytes;
        if remaining != GenerationAccounting::default() {
            usage.reserved_by_key.insert(key, remaining);
        } else {
            usage.reserved_by_key.remove(&key);
        }
    }

    fn reserve_fold_opening_usage(
        self: &Arc<Self>,
        key: StreamWorkerKey,
    ) -> Result<OpeningFoldUsagePermit, MemWalWorkerError> {
        let accounting = GenerationAccounting {
            rows: 0,
            arrow_bytes: B1_MAX_GENERATION_ARROW_BYTES,
            batches: 0,
        };
        let worker_usage = WorkerUsage {
            resident: true,
            accounting,
            pending_generations: 1,
            generation_registered: true,
        };
        let mut usage = self.usage.lock().expect("MemWAL registry usage poisoned");
        if usage.fold_required_by_key.contains_key(&key) {
            return Err(MemWalWorkerError::state(format!(
                "cold fold opening reservation already exists for {key}"
            )));
        }
        let root_actual = usage
            .resident_writers_root
            .checked_add(1)
            .ok_or_else(|| MemWalWorkerError::state("resident-writer root overflow"))?;
        if root_actual > self.limits.max_resident_writers_root {
            return Err(MemWalWorkerError::ResourceLimit {
                resource: "stream_resident_writers_root",
                limit: self.limits.max_resident_writers_root as u64,
                actual: root_actual as u64,
            });
        }
        let table_actual = usage
            .resident_writers_by_table
            .get(&key.identity)
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| MemWalWorkerError::state("resident-writer table overflow"))?;
        if table_actual > self.limits.max_resident_writers_per_table {
            return Err(MemWalWorkerError::ResourceLimit {
                resource: "stream_resident_writers_per_table",
                limit: self.limits.max_resident_writers_per_table as u64,
                actual: table_actual as u64,
            });
        }
        let bytes = usage
            .reserved_arrow_bytes
            .checked_add(accounting.arrow_bytes)
            .ok_or_else(|| MemWalWorkerError::state("fold opening Arrow reservation overflow"))?;
        if bytes > self.limits.max_reserved_arrow_bytes {
            return Err(MemWalWorkerError::ResourceLimit {
                resource: "stream_reserved_arrow_bytes",
                limit: self.limits.max_reserved_arrow_bytes,
                actual: bytes,
            });
        }
        let pending = usage
            .pending_generations
            .checked_add(1)
            .ok_or_else(|| MemWalWorkerError::state("fold opening pending overflow"))?;
        if pending > self.limits.max_pending_generations {
            return Err(MemWalWorkerError::ResourceLimit {
                resource: "stream_pending_generations",
                limit: self.limits.max_pending_generations as u64,
                actual: pending as u64,
            });
        }
        let by_key = usage
            .reserved_by_key
            .get(&key)
            .copied()
            .unwrap_or_default()
            .checked_add(accounting)?;
        if !by_key.fits() {
            return Err(MemWalWorkerError::ResourceLimit {
                resource: "stream_reserved_arrow_bytes",
                limit: B1_MAX_GENERATION_ARROW_BYTES,
                actual: by_key.arrow_bytes,
            });
        }

        usage.resident_writers_root = root_actual;
        usage
            .resident_writers_by_table
            .insert(key.identity, table_actual);
        usage.reserved_arrow_bytes = bytes;
        usage.pending_generations = pending;
        usage.reserved_by_key.insert(key, by_key);
        usage.fold_required_by_key.insert(key, accounting);
        Ok(OpeningFoldUsagePermit {
            registry: Arc::clone(self),
            key,
            usage: worker_usage,
            transferred: false,
        })
    }

    fn transfer_fold_opening_usage(
        &self,
        worker: &MemWalWorker,
        fold_required: GenerationAccounting,
    ) -> Result<(), MemWalWorkerError> {
        let mut usage = self.usage.lock().expect("MemWAL registry usage poisoned");
        let mut worker_usage = worker.usage.lock().expect("MemWAL worker usage poisoned");
        if worker_usage.generation_registered {
            return Err(MemWalWorkerError::state(
                "cold fold worker usage was already registered",
            ));
        }
        if !worker_usage.accounting.fits() {
            return Err(MemWalWorkerError::state(format!(
                "cold fold observed an oversized generation: {:?}",
                worker_usage.accounting
            )));
        }
        let opening = GenerationAccounting {
            rows: 0,
            arrow_bytes: B1_MAX_GENERATION_ARROW_BYTES,
            batches: 0,
        };
        let current = usage
            .reserved_by_key
            .get(&worker.key)
            .copied()
            .ok_or_else(|| MemWalWorkerError::state("cold fold opening reservation is absent"))?;
        if current != opening {
            return Err(MemWalWorkerError::state(format!(
                "cold fold opening reservation changed: expected {opening:?}, observed {current:?}"
            )));
        }
        let promoted = GenerationAccounting {
            rows: worker_usage.accounting.rows,
            arrow_bytes: B1_MAX_GENERATION_ARROW_BYTES,
            batches: worker_usage.accounting.batches,
        };
        usage.reserved_by_key.insert(worker.key, promoted);
        usage.fold_required_by_key.insert(worker.key, fold_required);
        worker_usage.accounting = promoted;
        worker_usage.pending_generations = 1;
        worker_usage.generation_registered = true;
        Ok(())
    }

    fn reserve_resident(&self, identity: TableIdentity) -> Result<(), MemWalWorkerError> {
        let mut usage = self.usage.lock().expect("MemWAL registry usage poisoned");
        let root_actual = usage.resident_writers_root.saturating_add(1);
        if root_actual > self.limits.max_resident_writers_root {
            return Err(MemWalWorkerError::ResourceLimit {
                resource: "stream_resident_writers_root",
                limit: self.limits.max_resident_writers_root as u64,
                actual: root_actual as u64,
            });
        }
        let table_actual = usage
            .resident_writers_by_table
            .get(&identity)
            .copied()
            .unwrap_or(0)
            .saturating_add(1);
        if table_actual > self.limits.max_resident_writers_per_table {
            return Err(MemWalWorkerError::ResourceLimit {
                resource: "stream_resident_writers_per_table",
                limit: self.limits.max_resident_writers_per_table as u64,
                actual: table_actual as u64,
            });
        }
        usage.resident_writers_root = root_actual;
        usage
            .resident_writers_by_table
            .insert(identity, table_actual);
        Ok(())
    }

    fn release_resident_reservation(&self, identity: TableIdentity) {
        let mut usage = self.usage.lock().expect("MemWAL registry usage poisoned");
        usage.resident_writers_root = usage.resident_writers_root.saturating_sub(1);
        if let Some(count) = usage.resident_writers_by_table.get_mut(&identity) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                usage.resident_writers_by_table.remove(&identity);
            }
        }
    }

    fn register_initial_worker_usage(
        &self,
        worker: &MemWalWorker,
    ) -> Result<(), MemWalWorkerError> {
        let mut usage = self.usage.lock().expect("MemWAL registry usage poisoned");
        let mut worker_usage = worker.usage.lock().expect("MemWAL worker usage poisoned");
        if worker_usage.generation_registered {
            return Ok(());
        }
        let initial = *worker_usage;
        if !initial.accounting.fits() {
            return Err(MemWalWorkerError::state(format!(
                "initial worker accounting exceeds the B1 generation cap: {:?}",
                initial.accounting
            )));
        }
        let bytes = usage
            .reserved_arrow_bytes
            .checked_add(initial.accounting.arrow_bytes)
            .ok_or_else(|| MemWalWorkerError::state("aggregate Arrow reservation overflow"))?;
        let pending = usage
            .pending_generations
            .checked_add(initial.pending_generations)
            .ok_or_else(|| MemWalWorkerError::state("pending-generation reservation overflow"))?;
        let by_key = usage
            .reserved_by_key
            .get(&worker.key)
            .copied()
            .unwrap_or_default()
            .checked_add(initial.accounting)?;

        // A cold claim can materialize replay before Lance exposes its exact
        // size. Once observed, count it immediately even when already-queued
        // caller buffers make the transient total exceed a configured cap.
        // The fold-required marker prevents any new charge; existing callers
        // drain until the aggregate and per-key projections converge.
        usage.reserved_arrow_bytes = bytes;
        usage.pending_generations = pending;
        if by_key != GenerationAccounting::default() {
            usage.reserved_by_key.insert(worker.key, by_key);
        }
        worker_usage.generation_registered = true;
        Ok(())
    }

    fn mark_fold_required(
        &self,
        key: StreamWorkerKey,
        accounting: GenerationAccounting,
    ) -> Result<(), MemWalWorkerError> {
        let mut usage = self.usage.lock().expect("MemWAL registry usage poisoned");
        match usage.fold_required_by_key.get(&key) {
            Some(existing) if *existing != accounting => Err(MemWalWorkerError::state(format!(
                "fold-required accounting changed for {key}: existing={existing:?}, observed={accounting:?}"
            ))),
            Some(_) => Ok(()),
            None => {
                usage.fold_required_by_key.insert(key, accounting);
                Ok(())
            }
        }
    }

    fn install_worker_usage(
        &self,
        worker: &MemWalWorker,
        fold_required: Option<GenerationAccounting>,
    ) -> Result<(), MemWalWorkerError> {
        if let Some(accounting) = fold_required {
            self.mark_fold_required(worker.key, accounting)?;
        }
        self.register_initial_worker_usage(worker)
    }

    fn install_conservative_retiring_usage(
        &self,
        worker: &MemWalWorker,
    ) -> Result<(), MemWalWorkerError> {
        self.install_worker_usage(
            worker,
            Some(GenerationAccounting {
                rows: 0,
                arrow_bytes: B1_MAX_GENERATION_ARROW_BYTES,
                batches: 0,
            }),
        )
    }

    fn transfer_batch_usage(
        &self,
        worker: &MemWalWorker,
        current: GenerationAccounting,
        charge: GenerationAccounting,
    ) -> OmniResult<()> {
        let mut registry_usage = self.usage.lock().expect("MemWAL registry usage poisoned");
        if registry_usage.reserved_arrow_bytes < charge.arrow_bytes {
            return Err(OmniError::manifest_internal(
                "queued stream batch charge is absent from aggregate reservation",
            ));
        }
        let adds_pending = usize::from(current.batches == 0);
        let pending = registry_usage
            .pending_generations
            .saturating_add(adds_pending);
        if pending > self.limits.max_pending_generations {
            return Err(Self::resource_error(
                "stream_pending_generations",
                self.limits.max_pending_generations as u64,
                pending as u64,
            ));
        }
        let mut worker_usage = worker.usage.lock().expect("MemWAL worker usage poisoned");
        if !worker_usage.generation_registered {
            return Err(OmniError::manifest_internal(
                "resident stream generation was not registered before transfer",
            ));
        }
        if worker_usage.accounting != current {
            return Err(OmniError::manifest_internal(format!(
                "resident stream reservation {:?} disagrees with generation accounting {current:?}",
                worker_usage.accounting
            )));
        }
        let worker_accounting = worker_usage
            .accounting
            .checked_add(charge)
            .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
        if !worker_accounting.fits() {
            return Err(OmniError::manifest_internal(format!(
                "worker reservation {worker_accounting:?} exceeds the prevalidated B1 cap"
            )));
        }
        let by_key = registry_usage
            .reserved_by_key
            .get(&worker.key)
            .copied()
            .unwrap_or_default();
        if by_key.rows < worker_accounting.rows
            || by_key.arrow_bytes < worker_accounting.arrow_bytes
            || by_key.batches < worker_accounting.batches
        {
            return Err(OmniError::manifest_internal(format!(
                "per-key stream reservation {by_key:?} does not contain transferred worker accounting {worker_accounting:?}"
            )));
        }
        registry_usage.pending_generations = pending;
        worker_usage.accounting = worker_accounting;
        worker_usage.pending_generations = worker_usage
            .pending_generations
            .saturating_add(adds_pending);
        Ok(())
    }

    /// Promote the exact resident-generation charge to the conservative full
    /// B1 cap before any seal/fresh-scan effect. On success this reservation is
    /// transferred into `SealedGenerationCut` and lives through publication.
    fn reserve_fold_usage(&self, worker: &MemWalWorker) -> Result<(), MemWalWorkerError> {
        let mut registry_usage = self.usage.lock().expect("MemWAL registry usage poisoned");
        let mut worker_usage = worker.usage.lock().expect("MemWAL worker usage poisoned");
        promote_fold_usage_counters(
            &mut registry_usage,
            &mut worker_usage,
            worker.key,
            &self.limits,
        )
    }

    fn release_worker_usage(&self, worker: &MemWalWorker) {
        let mut worker_usage = worker.usage.lock().expect("MemWAL worker usage poisoned");
        if !worker_usage.resident {
            return;
        }
        let released = *worker_usage;
        *worker_usage = WorkerUsage::default();
        drop(worker_usage);
        if released.generation_registered {
            self.release_usage(worker.key, released);
        } else {
            self.release_unregistered_resident(worker.key);
        }
    }

    fn release_unregistered_resident(&self, key: StreamWorkerKey) {
        let mut usage = self.usage.lock().expect("MemWAL registry usage poisoned");
        let table_count = usage
            .resident_writers_by_table
            .get(&key.identity)
            .copied()
            .unwrap_or(0);
        if usage.resident_writers_root == 0 || table_count == 0 {
            debug_assert!(false, "unregistered resident accounting release underflow");
            return;
        }
        usage.resident_writers_root -= 1;
        if let Some(count) = usage.resident_writers_by_table.get_mut(&key.identity) {
            *count -= 1;
            if *count == 0 {
                usage.resident_writers_by_table.remove(&key.identity);
            }
        }
        usage.fold_required_by_key.remove(&key);
    }

    fn release_usage(&self, key: StreamWorkerKey, released: WorkerUsage) {
        debug_assert!(released.generation_registered);
        let mut usage = self.usage.lock().expect("MemWAL registry usage poisoned");
        let current = usage.reserved_by_key.get(&key).copied().unwrap_or_default();
        let remaining = match current.checked_sub(released.accounting) {
            Ok(remaining) => remaining,
            Err(error) => {
                debug_assert!(false, "worker per-key accounting release failed: {error}");
                return;
            }
        };
        let table_count = usage
            .resident_writers_by_table
            .get(&key.identity)
            .copied()
            .unwrap_or(0);
        if usage.resident_writers_root == 0
            || table_count == 0
            || usage.reserved_arrow_bytes < released.accounting.arrow_bytes
            || usage.pending_generations < released.pending_generations
        {
            debug_assert!(false, "worker aggregate accounting release underflow");
            return;
        }
        usage.resident_writers_root -= 1;
        usage.reserved_arrow_bytes -= released.accounting.arrow_bytes;
        usage.pending_generations -= released.pending_generations;
        if remaining != GenerationAccounting::default() {
            usage.reserved_by_key.insert(key, remaining);
        } else {
            usage.reserved_by_key.remove(&key);
        }
        if let Some(count) = usage.resident_writers_by_table.get_mut(&key.identity) {
            *count -= 1;
            if *count == 0 {
                usage.resident_writers_by_table.remove(&key.identity);
            }
        }
        usage.fold_required_by_key.remove(&key);
    }

    fn take_worker_usage(&self, worker: &MemWalWorker) -> WorkerUsage {
        let mut usage = worker.usage.lock().expect("MemWAL worker usage poisoned");
        std::mem::take(&mut *usage)
    }

    /// Run the authority acquisition/check and complete append in a detached
    /// task. Dropping the requesting future drops only the result receiver; it
    /// never cancels the check, put, watcher, or retirement.
    pub(crate) async fn put(
        self: &Arc<Self>,
        key: StreamWorkerKey,
        table_key: String,
        batch: RecordBatch,
        caller_ordinals: CallerOrdinalRange,
        confirmed_token_updates: ConfirmedStreamTokenOverlay,
        mut queued: QueuedBatchPermit,
        prepare: PreparePut,
        idle_authority: IdleAuthorityCheck,
        fold_trigger: StreamFoldTrigger,
    ) -> OmniResult<DurableBatchAck> {
        if queued.key != key {
            return Err(OmniError::manifest_internal(format!(
                "queued stream permit {} was supplied for worker {key}",
                queued.key
            )));
        }
        let slot = self.slot(key);
        let inflight = queued.take_inflight();
        let registry = Arc::clone(self);
        crate::instrumentation::spawn_with_query_io_probes(async move {
            let _inflight = inflight;
            registry
                .put_background(
                    slot,
                    key,
                    table_key,
                    batch,
                    caller_ordinals,
                    confirmed_token_updates,
                    queued,
                    prepare,
                    idle_authority,
                    fold_trigger,
                )
                .await
        })
        .await
        .map_err(|error| OmniError::Lance(format!("MemWAL put task failed: {error}")))?
    }

    async fn put_background(
        self: Arc<Self>,
        slot: Arc<RegistrySlot>,
        key: StreamWorkerKey,
        table_key: String,
        batch: RecordBatch,
        caller_ordinals: CallerOrdinalRange,
        confirmed_token_updates: ConfirmedStreamTokenOverlay,
        queued: QueuedBatchPermit,
        prepare: PreparePut,
        idle_authority: IdleAuthorityCheck,
        fold_trigger: StreamFoldTrigger,
    ) -> OmniResult<DurableBatchAck> {
        crate::failpoints::maybe_fail(
            crate::failpoints::names::STREAM_B1_AFTER_INPUT_QUEUE_BEFORE_PREPARE,
        )?;
        let mut prepare = Some(prepare);
        let (worker, authority) = loop {
            let (existing, opening) = {
                let mut slot_state = slot.state.lock().await;
                match &*slot_state {
                    RegistrySlotState::Vacant => {
                        self.reserve_resident(key.identity)
                            .map_err(Self::worker_error_to_omni)?;
                        let opening = Arc::new(tokio::sync::Notify::new());
                        *slot_state = RegistrySlotState::Opening(Arc::clone(&opening));
                        (None, Some(opening))
                    }
                    RegistrySlotState::Opening(opening) => {
                        let notified = Arc::clone(opening).notified_owned();
                        drop(slot_state);
                        notified.await;
                        continue;
                    }
                    RegistrySlotState::Active(worker) => (Some(Arc::clone(worker)), None),
                    RegistrySlotState::Retiring(_) => {
                        return Err(OmniError::Lance(
                            MemWalWorkerError::Retiring {
                                key,
                                reason: "original abort completion has not settled".to_string(),
                            }
                            .to_string(),
                        ));
                    }
                }
            };

            // Never hold the slot mutex while acquiring the shared admission
            // lease or performing the fresh authority check. An exclusive
            // fold may already hold that lease and need the slot to reject an
            // in-progress cold claim without deadlocking.
            let prepared = prepare
                .take()
                .expect("prepare is consumed only after opening waiters settle")(
                existing.as_ref().map(|worker| Arc::clone(&worker.writer)),
            )
            .await;
            let mut slot_state = slot.state.lock().await;
            match (opening, existing, prepared) {
                (Some(opening), None, Ok(PreparedPut::Cold { authority, opened })) => {
                    if !matches!(&*slot_state, RegistrySlotState::Opening(current) if Arc::ptr_eq(current, &opening))
                    {
                        let fold_required = opened.fold_required_accounting();
                        let worker = Arc::new(MemWalWorker::from_opened(key, table_key, opened));
                        let install = self.install_worker_usage(&worker, fold_required);
                        *slot_state = RegistrySlotState::Active(Arc::clone(&worker));
                        opening.notify_waiters();
                        drop(slot_state);
                        *worker.mode.lock().await = WorkerMode::Retiring;
                        self.retain_shared_retirement(&slot, &worker, authority)
                            .await;
                        if let Err(error) = install {
                            return Err(Self::worker_error_to_omni(error));
                        }
                        return Err(OmniError::Lance(
                            "MemWAL opening reservation changed during cold prepare; claimed writer is retiring"
                                .to_string(),
                        ));
                    }
                    let cold_refusal = opened.put_refusal(&table_key);
                    let fold_required = opened.fold_required_accounting();
                    let worker = Arc::new(MemWalWorker::from_opened(key, table_key, opened));
                    *slot_state = RegistrySlotState::Active(Arc::clone(&worker));
                    if let Some(accounting) = fold_required
                        && let Err(error) = self.mark_fold_required(key, accounting)
                    {
                        opening.notify_waiters();
                        drop(slot_state);
                        *worker.mode.lock().await = WorkerMode::Retiring;
                        self.retain_shared_retirement(&slot, &worker, authority)
                            .await;
                        return Err(Self::worker_error_to_omni(error));
                    }
                    if let Err(error) = self.register_initial_worker_usage(&worker) {
                        opening.notify_waiters();
                        drop(slot_state);
                        *worker.mode.lock().await = WorkerMode::Retiring;
                        self.retain_shared_retirement(&slot, &worker, authority)
                            .await;
                        return Err(Self::worker_error_to_omni(error));
                    }
                    opening.notify_waiters();
                    if let Some(error) = cold_refusal {
                        if matches!(&error, OmniError::FoldRequired { .. }) {
                            fold_trigger(true);
                        }
                        // Account the replay first, then free this opener's
                        // caller buffer and queue position. Already-charged
                        // callers observe the fold-only worker and drain;
                        // new callers are refused by the marker before they
                        // can add charge. Return immediately after releasing
                        // this queue/authority: waiting for root-global budget
                        // convergence while holding this key's shared gate can
                        // cycle with a sorted multi-key exclusive recovery.
                        drop(batch);
                        drop(queued);
                        drop(slot_state);
                        self.ensure_idle_task(&slot, &worker, idle_authority);
                        return Err(error);
                    }
                    break (worker, authority);
                }
                (Some(opening), None, Ok(PreparedPut::Warm { .. })) => {
                    if matches!(&*slot_state, RegistrySlotState::Opening(current) if Arc::ptr_eq(current, &opening))
                    {
                        *slot_state = RegistrySlotState::Vacant;
                        opening.notify_waiters();
                        self.release_resident_reservation(key.identity);
                    }
                    return Err(OmniError::Lance(
                        "cold MemWAL prepare returned a warm outcome without claiming a writer"
                            .to_string(),
                    ));
                }
                (Some(opening), None, Err(failure)) => {
                    let (error, authority, failed_worker) = failure.into_parts();
                    let Some(failed_worker) = failed_worker else {
                        if matches!(&*slot_state, RegistrySlotState::Opening(current) if Arc::ptr_eq(current, &opening))
                        {
                            *slot_state = RegistrySlotState::Vacant;
                            opening.notify_waiters();
                            self.release_resident_reservation(key.identity);
                        }
                        return Err(error);
                    };
                    let (worker, install) = match failed_worker {
                        FailedColdWorker::Claimed(claimed) => {
                            let worker = Arc::new(MemWalWorker::retiring_from_claimed(
                                key, table_key, claimed,
                            ));
                            let install = self.install_conservative_retiring_usage(&worker);
                            (worker, install)
                        }
                        FailedColdWorker::Opened(opened) => {
                            let fold_required = opened.fold_required_accounting();
                            let worker =
                                Arc::new(MemWalWorker::from_opened(key, table_key, opened));
                            let install = self.install_worker_usage(&worker, fold_required);
                            (worker, install)
                        }
                    };
                    *slot_state = RegistrySlotState::Active(Arc::clone(&worker));
                    opening.notify_waiters();
                    drop(slot_state);
                    *worker.mode.lock().await = WorkerMode::Retiring;
                    self.retain_shared_retirement(&slot, &worker, authority)
                        .await;
                    match install {
                        Ok(()) => return Err(error),
                        Err(install_error) => {
                            return Err(Self::worker_error_to_omni(install_error));
                        }
                    }
                }
                (None, Some(expected), Ok(PreparedPut::Warm { authority })) => {
                    let same = matches!(
                        &*slot_state,
                        RegistrySlotState::Active(worker) if Arc::ptr_eq(worker, &expected)
                    );
                    if !same {
                        *expected.mode.lock().await = WorkerMode::Retiring;
                        drop(slot_state);
                        self.retain_shared_retirement(&slot, &expected, authority)
                            .await;
                        return Err(OmniError::Lance(
                            "warm MemWAL worker changed during authority acquisition; original writer is retiring"
                                .to_string(),
                        ));
                    }
                    break (expected, authority);
                }
                (None, Some(expected), Ok(PreparedPut::Cold { authority, opened })) => {
                    *expected.mode.lock().await = WorkerMode::Retiring;
                    let extra = opened.into_claimed().writer;
                    drop(slot_state);
                    self.retain_shared_retirement_with_extra(&slot, &expected, authority, extra)
                        .await;
                    return Err(OmniError::Lance(
                        "warm MemWAL prepare attempted a second writer claim; both writers are retiring"
                            .to_string(),
                    ));
                }
                (None, Some(expected), Err(failure)) => {
                    let (error, authority, failed_worker) = failure.into_parts();
                    *expected.mode.lock().await = WorkerMode::Retiring;
                    drop(slot_state);
                    match failed_worker {
                        Some(FailedColdWorker::Claimed(claimed)) => {
                            self.retain_shared_retirement_with_extra(
                                &slot,
                                &expected,
                                authority,
                                claimed.writer,
                            )
                            .await;
                        }
                        Some(FailedColdWorker::Opened(opened)) => {
                            self.retain_shared_retirement_with_extra(
                                &slot,
                                &expected,
                                authority,
                                opened.into_claimed().writer,
                            )
                            .await;
                        }
                        None => {
                            self.retain_shared_retirement(&slot, &expected, authority)
                                .await;
                        }
                    }
                    return Err(error);
                }
                _ => {
                    return Err(OmniError::Lance(
                        "invalid MemWAL opening reservation transition".to_string(),
                    ));
                }
            }
        };

        self.ensure_idle_task(&slot, &worker, idle_authority);
        self.invoke_put(
            &slot,
            &worker,
            batch,
            caller_ordinals,
            confirmed_token_updates,
            authority,
            queued,
            fold_trigger,
        )
        .await
    }

    async fn invoke_put(
        self: &Arc<Self>,
        slot: &Arc<RegistrySlot>,
        worker: &Arc<MemWalWorker>,
        batch: RecordBatch,
        caller_ordinals: CallerOrdinalRange,
        confirmed_token_updates: ConfirmedStreamTokenOverlay,
        authority: CheckedStreamAuthority,
        queued: QueuedBatchPermit,
        fold_trigger: StreamFoldTrigger,
    ) -> OmniResult<DurableBatchAck> {
        // Any prepared use—not only a successful append—extends the idle
        // deadline. This prevents the single eviction task from racing an
        // effect-free validation/budget refusal on the same warm worker.
        *worker
            .last_used
            .lock()
            .expect("MemWAL worker idle clock poisoned") = std::time::Instant::now();
        let row_count = u64::try_from(batch.num_rows())
            .map_err(|_| OmniError::Lance("stream batch row count overflow".to_string()))?;
        if caller_ordinals
            .len()
            .map_err(|error| OmniError::Lance(error.to_string()))?
            != row_count
        {
            return Err(OmniError::Lance(format!(
                "caller ordinal range {:?} does not match {row_count} batch rows",
                caller_ordinals.as_range()
            )));
        }
        for (logical_id, update) in &confirmed_token_updates {
            update.validate(worker.key.identity, logical_id)?;
        }
        let charge = queued.charge();
        let mut mode = worker.mode.lock().await;
        let current = match &*mode {
            WorkerMode::Admit(accounting) => *accounting,
            WorkerMode::FoldReplay(replay) => {
                fold_trigger(true);
                return Err(OmniError::FoldRequired {
                    table_key: worker.table_key.clone(),
                    rows: replay.accounting.rows,
                    bytes: replay.accounting.arrow_bytes,
                });
            }
            WorkerMode::FoldFlushed(_) => {
                fold_trigger(true);
                return Err(OmniError::FoldRequired {
                    table_key: worker.table_key.clone(),
                    rows: 0,
                    bytes: 0,
                });
            }
            WorkerMode::Retiring => {
                return Err(OmniError::Lance(format!(
                    "MemWAL worker {} is retiring",
                    worker.key
                )));
            }
        };
        let reserved = current
            .checked_add(charge)
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        if !reserved.fits() {
            fold_trigger(true);
            return Err(OmniError::FoldRequired {
                table_key: worker.table_key.clone(),
                rows: reserved.rows,
                bytes: reserved.arrow_bytes,
            });
        }

        let invocation_deadline = self.limits.put_deadline;
        let deadline = tokio::time::Instant::now() + invocation_deadline;

        crate::failpoints::maybe_fail(crate::failpoints::names::STREAM_B1_BEFORE_PUT_INVOKE)?;
        queued.transfer_to_worker(worker, current)?;
        // `put_no_wait` is itself background-owned.  The deadline bounds only
        // how long this acknowledgement waits; timing out detaches a
        // continuation which retains admission until the Lance future settles
        // and only then starts the one abort.
        let writer = Arc::clone(&worker.writer);
        let mut invoked = tokio::spawn(async move { writer.put_no_wait(vec![batch]).await });
        // This callback lives inside the detached owner and fires once the
        // physical invocation itself has an owner. The eventual readiness
        // probe distinguishes a durable/ambiguous tail from a no-effect
        // failure, while caller cancellation cannot erase the trigger.
        fold_trigger(
            reserved.rows == B1_MAX_GENERATION_ROWS
                || reserved.arrow_bytes == B1_MAX_GENERATION_ARROW_BYTES,
        );
        let (write_result, watcher) = match tokio::time::timeout_at(deadline, &mut invoked).await {
            Ok(Ok(Ok(value))) => value,
            Ok(Ok(Err(error))) => {
                *mode = WorkerMode::Retiring;
                drop(mode);
                let unknown = worker.ack_unknown(
                    caller_ordinals,
                    format!("put_no_wait returned after invocation: {error}"),
                );
                self.retain_shared_retirement(slot, worker, authority).await;
                return Err(unknown);
            }
            Ok(Err(error)) => {
                *mode = WorkerMode::Retiring;
                drop(mode);
                let unknown = worker.ack_unknown(
                    caller_ordinals,
                    format!("put_no_wait owner task failed after invocation: {error}"),
                );
                self.retain_shared_retirement(slot, worker, authority).await;
                return Err(unknown);
            }
            Err(_) => {
                *mode = WorkerMode::Retiring;
                drop(mode);
                let unknown = worker.ack_unknown(
                    caller_ordinals,
                    format!(
                        "put_no_wait did not settle within {} ms after invocation",
                        invocation_deadline.as_millis()
                    ),
                );
                self.retain_shared_after_settle(slot, worker, authority, async move {
                    // A late successful invocation transfers ownership of its
                    // watcher too. Settle it before abort; never turn an
                    // acknowledgement timeout into a dropped durability
                    // observation.
                    if let Ok(Ok((_, Some(mut watcher)))) = invoked.await {
                        let _ = watcher.wait().await;
                    }
                });
                return Err(unknown);
            }
        };
        if let Err(error) = crate::failpoints::maybe_fail(
            crate::failpoints::names::STREAM_B1_AFTER_PUT_INVOKE_BEFORE_WATCHER,
        ) {
            *mode = WorkerMode::Retiring;
            drop(mode);
            let unknown = worker.ack_unknown(caller_ordinals, error.to_string());
            match watcher {
                Some(mut watcher) => {
                    self.retain_shared_after_settle(slot, worker, authority, async move {
                        let _ = watcher.wait().await;
                    })
                }
                None => self.retain_shared_retirement(slot, worker, authority).await,
            }
            return Err(unknown);
        }
        if write_result.batch_positions.end != write_result.batch_positions.start.saturating_add(1)
        {
            *mode = WorkerMode::Retiring;
            drop(mode);
            let unknown = worker.ack_unknown(
                caller_ordinals,
                "put_no_wait returned a non-singleton batch-position interval",
            );
            match watcher {
                Some(mut watcher) => {
                    self.retain_shared_after_settle(slot, worker, authority, async move {
                        let _ = watcher.wait().await;
                    })
                }
                None => self.retain_shared_retirement(slot, worker, authority).await,
            }
            return Err(unknown);
        }
        let Some(mut watcher) = watcher else {
            *mode = WorkerMode::Retiring;
            drop(mode);
            let unknown = worker.ack_unknown(
                caller_ordinals,
                "durable writer returned no BatchDurableWatcher",
            );
            self.retain_shared_retirement(slot, worker, authority).await;
            return Err(unknown);
        };
        if let Err(error) =
            crate::failpoints::maybe_fail(crate::failpoints::names::STREAM_B1_BEFORE_WATCHER_WAIT)
        {
            *mode = WorkerMode::Retiring;
            drop(mode);
            let unknown = worker.ack_unknown(caller_ordinals, error.to_string());
            self.retain_shared_after_settle(slot, worker, authority, async move {
                let _ = watcher.wait().await;
            });
            return Err(unknown);
        }
        let mut watcher_task = tokio::spawn(async move { watcher.wait().await });
        match tokio::time::timeout_at(deadline, &mut watcher_task).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => {
                *mode = WorkerMode::Retiring;
                drop(mode);
                let unknown = worker.ack_unknown(
                    caller_ordinals,
                    format!("durability watcher failed: {error}"),
                );
                self.retain_shared_retirement(slot, worker, authority).await;
                return Err(unknown);
            }
            Ok(Err(error)) => {
                *mode = WorkerMode::Retiring;
                drop(mode);
                let unknown = worker.ack_unknown(
                    caller_ordinals,
                    format!("durability watcher owner task failed: {error}"),
                );
                self.retain_shared_retirement(slot, worker, authority).await;
                return Err(unknown);
            }
            Err(_) => {
                *mode = WorkerMode::Retiring;
                drop(mode);
                let unknown = worker.ack_unknown(
                    caller_ordinals,
                    format!(
                        "durability watcher did not settle within the {} ms invocation deadline",
                        invocation_deadline.as_millis()
                    ),
                );
                self.retain_shared_after_settle(slot, worker, authority, async move {
                    let _ = watcher_task.await;
                });
                return Err(unknown);
            }
        }
        if let Err(error) =
            crate::failpoints::maybe_fail(crate::failpoints::names::STREAM_B1_AFTER_WATCHER_SUCCESS)
        {
            *mode = WorkerMode::Retiring;
            drop(mode);
            let unknown = worker.ack_unknown(caller_ordinals, error.to_string());
            self.retain_shared_retirement(slot, worker, authority).await;
            return Err(unknown);
        }

        // RC.1 checks shard-epoch fencing after a WAL PUT collision or error,
        // but not after a successful PUT.  The durability watcher therefore
        // proves persistence, not that no successor writer has claimed the
        // shard at the acknowledgement boundary.  Recheck through Lance's
        // public surface before returning a clean ack.  `check_fenced` treats a
        // missing manifest as unfenced, which is safe only under B1's separate
        // prohibition on raw `_mem_wal` deletion.  Any failure is
        // post-invocation: the row may be durable, so retire the worker and
        // report AckUnknown rather than presenting an effect-free fence or
        // retry signal.
        let fence_writer = Arc::clone(&worker.writer);
        let mut fence_task = tokio::spawn(async move { fence_writer.check_fenced().await });
        match tokio::time::timeout_at(deadline, &mut fence_task).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => {
                *mode = WorkerMode::Retiring;
                drop(mode);
                let unknown = worker.ack_unknown(
                    caller_ordinals,
                    format!("post-durability writer fence check failed: {error}"),
                );
                self.retain_shared_retirement(slot, worker, authority).await;
                return Err(unknown);
            }
            Ok(Err(error)) => {
                *mode = WorkerMode::Retiring;
                drop(mode);
                let unknown = worker.ack_unknown(
                    caller_ordinals,
                    format!("post-durability writer fence owner task failed: {error}"),
                );
                self.retain_shared_retirement(slot, worker, authority).await;
                return Err(unknown);
            }
            Err(_) => {
                *mode = WorkerMode::Retiring;
                drop(mode);
                let unknown = worker.ack_unknown(
                    caller_ordinals,
                    format!(
                        "post-durability writer fence check did not settle within the {} ms invocation deadline",
                        invocation_deadline.as_millis()
                    ),
                );
                self.retain_shared_after_settle(slot, worker, authority, async move {
                    let _ = fence_task.await;
                });
                return Err(unknown);
            }
        }

        {
            let mut overlay = worker
                .confirmed_token_overlay
                .lock()
                .expect("MemWAL token overlay poisoned");
            overlay.extend(confirmed_token_updates);
        }
        *mode = WorkerMode::Admit(reserved);
        *worker
            .last_used
            .lock()
            .expect("MemWAL worker idle clock poisoned") = std::time::Instant::now();
        Ok(DurableBatchAck {
            identity: key_identity(worker.key),
            enrollment_id: worker.key.enrollment_id,
            shard_id: worker.key.shard_id,
            writer_epoch: worker.writer.epoch(),
            caller_ordinals,
            row_count,
        })
    }

    /// Retain admission while an already-invoked Lance operation settles.
    /// The settling future is owned by this detached task, so a caller timeout
    /// or cancellation cannot drop it.  Retirement begins only afterwards.
    fn retain_shared_after_settle<F>(
        self: &Arc<Self>,
        slot: &Arc<RegistrySlot>,
        worker: &Arc<MemWalWorker>,
        authority: CheckedStreamAuthority,
        settle: F,
    ) where
        F: Future<Output = ()> + Send + 'static,
    {
        let registry = Arc::clone(self);
        let slot = Arc::clone(slot);
        let worker = Arc::clone(worker);
        spawn_after_settle(authority, settle, move |authority| async move {
            registry
                .retain_shared_retirement(&slot, &worker, authority)
                .await;
        });
    }

    async fn begin_abort(
        &self,
        slot: &Arc<RegistrySlot>,
        worker: &Arc<MemWalWorker>,
    ) -> RetirementHandle {
        self.begin_abort_with_extra(slot, worker, None).await
    }

    async fn begin_abort_with_extra(
        &self,
        slot: &Arc<RegistrySlot>,
        worker: &Arc<MemWalWorker>,
        extra_writer: Option<Arc<ShardWriter>>,
    ) -> RetirementHandle {
        let mut state = slot.state.lock().await;
        if let RegistrySlotState::Retiring(handle) = &*state {
            debug_assert!(
                extra_writer.is_none(),
                "an extra claimed writer must be enrolled in the first retained abort"
            );
            return handle.clone();
        }
        let (sender, receiver) = watch::channel(None);
        let handle = RetirementHandle { receiver };
        *state = RegistrySlotState::Retiring(handle.clone());
        // Do not retain the graph-scoped idle authority factory until the
        // ordinary idle deadline after the slot has left Active. `notify_one`
        // keeps a permit when the spawned idle task has not started polling
        // yet, closing the install-then-immediate-quiesce race.
        worker.idle_retirement.notify_one();
        let writer = Arc::clone(&worker.writer);
        tokio::spawn(async move {
            let mut error = match crate::failpoints::maybe_fail(
                crate::failpoints::names::STREAM_B1_BEFORE_ABORT,
            )
            .and_then(|()| {
                crate::failpoints::maybe_fail(crate::failpoints::names::STREAM_B1_ABORT_STALL)
            }) {
                Ok(()) => writer.abort().await.err().map(|error| error.to_string()),
                Err(error) => Some(error.to_string()),
            };
            if let Some(extra_writer) = extra_writer {
                if let Err(extra_error) = extra_writer.abort().await {
                    let extra_error = format!("extra claimed writer abort failed: {extra_error}");
                    error = Some(match error {
                        Some(primary) => format!("{primary}; {extra_error}"),
                        None => extra_error,
                    });
                }
            }
            let _ = sender.send(Some(AbortOutcome { error }));
        });
        handle
    }

    async fn retain_shared_retirement(
        self: &Arc<Self>,
        slot: &Arc<RegistrySlot>,
        worker: &Arc<MemWalWorker>,
        authority: CheckedStreamAuthority,
    ) {
        self.retain_shared_retirement_inner(slot, worker, authority, None)
            .await;
    }

    async fn retain_shared_retirement_with_extra(
        self: &Arc<Self>,
        slot: &Arc<RegistrySlot>,
        worker: &Arc<MemWalWorker>,
        authority: CheckedStreamAuthority,
        extra_writer: Arc<ShardWriter>,
    ) {
        self.retain_shared_retirement_inner(slot, worker, authority, Some(extra_writer))
            .await;
    }

    async fn retain_shared_retirement_inner(
        self: &Arc<Self>,
        slot: &Arc<RegistrySlot>,
        worker: &Arc<MemWalWorker>,
        authority: CheckedStreamAuthority,
        extra_writer: Option<Arc<ShardWriter>>,
    ) {
        let handle = self
            .begin_abort_with_extra(slot, worker, extra_writer)
            .await;
        let slot = Arc::clone(slot);
        let worker = Arc::clone(worker);
        let registry = Arc::clone(self);
        let abort_deadline = self.limits.abort_deadline;
        tokio::spawn(async move {
            let _authority = authority;
            let completed = match tokio::time::timeout(abort_deadline, handle.clone().wait()).await
            {
                Ok(result) => result.is_ok(),
                Err(_) => handle.wait().await.is_ok(),
            };
            if completed {
                let mut state = slot.state.lock().await;
                if matches!(&*state, RegistrySlotState::Retiring(_)) {
                    *state = RegistrySlotState::Vacant;
                }
                drop(state);
                registry.release_worker_usage(&worker);
                drop(_authority);
                let _ = crate::failpoints::maybe_fail(
                    crate::failpoints::names::STREAM_B1_AFTER_RETIREMENT_RELEASE,
                );
            } else {
                // Abort failure is fail-closed for this process. Keeping this
                // task pending retains the shared admission token as well as
                // the Retiring registry entry.
                std::future::pending::<()>().await;
            }
        });
    }

    fn retain_failed_fold_open_join(
        &self,
        authority: CheckedExclusiveStreamAuthority,
        inflight: InFlightPermit,
        opening_usage: OpeningFoldUsagePermit,
    ) {
        tokio::spawn(async move {
            // The opener task died after it may have claimed/materialized a
            // writer but returned no ownership handle. Fail closed forever:
            // keep exclusive authority and every conservative reservation.
            let _retained = (authority, inflight, opening_usage);
            std::future::pending::<()>().await;
        });
    }

    fn retain_failed_fold_open_transfer(
        &self,
        authority: CheckedExclusiveStreamAuthority,
        inflight: InFlightPermit,
        opening_usage: OpeningFoldUsagePermit,
        worker: Arc<MemWalWorker>,
    ) {
        tokio::spawn(async move {
            // Lance may already have claimed/materialized this writer, but an
            // internal accounting invariant prevented the conservative
            // opening reservation from transferring to it. Neither owner can
            // be released safely: retain the raw writer, full reservation,
            // inflight permit, and exclusive admission forever. The registry
            // slot remains Opening, so every later operation also fails
            // closed instead of reusing the shard.
            let _retained = (authority, inflight, opening_usage, worker);
            std::future::pending::<()>().await;
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn retain_fold_open_after_timeout(
        self: &Arc<Self>,
        slot: Arc<RegistrySlot>,
        key: StreamWorkerKey,
        table_key: String,
        opening: Arc<tokio::sync::Notify>,
        mut opening_usage: OpeningFoldUsagePermit,
        authority: CheckedExclusiveStreamAuthority,
        inflight: InFlightPermit,
        open_task: tokio::task::JoinHandle<Result<OpenedMemWalWorker, WorkerOpenFailure>>,
    ) {
        let registry = Arc::clone(self);
        tokio::spawn(async move {
            let opened = match open_task.await {
                Ok(opened) => opened,
                Err(_) => {
                    let _retained = (authority, inflight, opening_usage);
                    std::future::pending::<()>().await;
                    unreachable!();
                }
            };

            let mut slot_state = slot.state.lock().await;
            if !matches!(&*slot_state, RegistrySlotState::Opening(current) if Arc::ptr_eq(current, &opening))
            {
                let _retained = (authority, inflight, opening_usage, opened, slot_state);
                std::future::pending::<()>().await;
                unreachable!();
            }
            let worker = match opened {
                Ok(opened) => {
                    let fold_required = opened
                        .fold_required_accounting()
                        .unwrap_or_else(full_generation_reservation);
                    let worker = Arc::new(MemWalWorker::from_opened(key, table_key, opened));
                    if opening_usage
                        .transfer_to_worker(&worker, fold_required)
                        .is_err()
                    {
                        let _retained = (authority, inflight, opening_usage, worker, slot_state);
                        std::future::pending::<()>().await;
                        unreachable!();
                    }
                    *worker.mode.lock().await = WorkerMode::Retiring;
                    worker
                }
                Err(failure) => {
                    let (_, claimed) = failure.into_parts();
                    let Some(claimed) = claimed else {
                        *slot_state = RegistrySlotState::Vacant;
                        opening.notify_waiters();
                        drop(slot_state);
                        drop(opening_usage);
                        return;
                    };
                    let worker =
                        Arc::new(MemWalWorker::retiring_from_claimed(key, table_key, claimed));
                    if opening_usage
                        .transfer_to_worker(&worker, full_generation_reservation())
                        .is_err()
                    {
                        let _retained = (authority, inflight, opening_usage, worker, slot_state);
                        std::future::pending::<()>().await;
                        unreachable!();
                    }
                    worker
                }
            };
            *slot_state = RegistrySlotState::Active(Arc::clone(&worker));
            opening.notify_waiters();
            drop(slot_state);

            let retirement = registry.begin_abort(&slot, &worker).await;
            if retirement.wait().await.is_ok() {
                let mut state = slot.state.lock().await;
                if matches!(&*state, RegistrySlotState::Retiring(_)) {
                    *state = RegistrySlotState::Vacant;
                }
                drop(state);
                registry.release_worker_usage(&worker);
                drop(authority);
                drop(inflight);
                let _ = crate::failpoints::maybe_fail(
                    crate::failpoints::names::STREAM_B1_AFTER_RETIREMENT_RELEASE,
                );
            } else {
                let _retained = (authority, inflight, worker);
                std::future::pending::<()>().await;
            }
        });
    }

    /// Arm at most one idle task for this exact cached worker. Activity moves
    /// `last_used`; the task recalculates that deadline instead of spawning one
    /// sleeper per acknowledgement. It asks graph orchestration for a fresh
    /// checked shared lease only after the local worker is both empty and idle.
    fn ensure_idle_task(
        self: &Arc<Self>,
        slot: &Arc<RegistrySlot>,
        worker: &Arc<MemWalWorker>,
        authority_check: IdleAuthorityCheck,
    ) {
        if worker.idle_task_armed.swap(true, Ordering::AcqRel) {
            return;
        }
        let registry = Arc::clone(self);
        let slot = Arc::clone(slot);
        let worker = Arc::clone(worker);
        tokio::spawn(async move {
            loop {
                let last_used = *worker
                    .last_used
                    .lock()
                    .expect("MemWAL worker idle clock poisoned");
                tokio::select! {
                    biased;
                    _ = worker.idle_retirement.notified() => break,
                    _ = tokio::time::sleep_until(tokio::time::Instant::from_std(
                        last_used + registry.limits.idle_timeout,
                    )) => {}
                }

                let is_same_active = {
                    let state = slot.state.lock().await;
                    matches!(
                        &*state,
                        RegistrySlotState::Active(current) if Arc::ptr_eq(current, &worker)
                    )
                };
                if !is_same_active {
                    break;
                }

                let mode = worker.mode.lock().await;
                let is_empty = matches!(
                    &*mode,
                    WorkerMode::Admit(accounting) if accounting.batches == 0
                );
                if !is_empty {
                    // A non-empty generation is retired by fold/ambiguity and
                    // never becomes empty in place, so no idle task remains.
                    break;
                }
                let is_idle = worker
                    .last_used
                    .lock()
                    .expect("MemWAL worker idle clock poisoned")
                    .elapsed()
                    >= registry.limits.idle_timeout;
                drop(mode);
                if !is_idle {
                    continue;
                }

                let authority = tokio::select! {
                    biased;
                    _ = worker.idle_retirement.notified() => break,
                    authority = authority_check(Arc::clone(&worker.writer)) => authority,
                };
                match authority {
                    Ok(authority) => match registry.evict_idle(worker.key, authority).await {
                        Ok(true) => break,
                        Ok(false) => continue,
                        Err(error) => {
                            tracing::warn!(
                                worker = %worker.key,
                                error = %error,
                                "idle MemWAL eviction could not start; retrying"
                            );
                            tokio::select! {
                                biased;
                                _ = worker.idle_retirement.notified() => break,
                                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                            }
                        }
                    },
                    Err(failure) => {
                        let (error, authority) = failure.into_parts();
                        *worker.mode.lock().await = WorkerMode::Retiring;
                        registry
                            .retain_shared_retirement(&slot, &worker, authority)
                            .await;
                        tracing::warn!(
                            worker = %worker.key,
                            error = %error,
                            "idle MemWAL authority changed; cached writer is retiring"
                        );
                        break;
                    }
                }
            }
            worker.idle_task_armed.store(false, Ordering::Release);
        });
    }

    /// Opportunistic idle eviction. The caller performs the same fresh check
    /// as append and transfers its shared admission token here; no handle-local
    /// timer may abort outside that common lock domain.
    pub(crate) async fn evict_idle(
        self: &Arc<Self>,
        key: StreamWorkerKey,
        authority: CheckedStreamAuthority,
    ) -> Result<bool, MemWalWorkerError> {
        let inflight = self.reserve_inflight_core()?;
        let registry = Arc::clone(self);
        tokio::spawn(async move {
            let _inflight = inflight;
            let slot = registry.slot(key);
            let worker = {
                let state = slot.state.lock().await;
                match &*state {
                    RegistrySlotState::Vacant => return Ok(false),
                    RegistrySlotState::Opening(_) => {
                        return Err(MemWalWorkerError::state(
                            "cannot evict while a cold claim is in progress",
                        ));
                    }
                    RegistrySlotState::Active(worker) => Arc::clone(worker),
                    RegistrySlotState::Retiring(_) => return Ok(false),
                }
            };
            let mut mode = worker.mode.lock().await;
            let empty = matches!(&*mode, WorkerMode::Admit(accounting) if accounting.batches == 0);
            let idle = worker
                .last_used
                .lock()
                .expect("MemWAL worker idle clock poisoned")
                .elapsed()
                >= registry.limits.idle_timeout;
            if !empty || !idle {
                return Ok(false);
            }
            *mode = WorkerMode::Retiring;
            drop(mode);
            registry
                .retain_shared_retirement(&slot, &worker, authority)
                .await;
            Ok(true)
        })
        .await
        .map_err(|error| MemWalWorkerError::state(format!("idle eviction task failed: {error}")))?
    }

    /// Seal exactly one generation, prove the authoritative drain result, and
    /// await the same retained abort task. The returned cut continues to own
    /// the exclusive admission token for the subsequent fold protocol.
    pub(crate) async fn seal_and_drain(
        self: &Arc<Self>,
        key: StreamWorkerKey,
        table_key: String,
        authority: CheckedExclusiveStreamAuthority,
        opener: WorkerOpener,
    ) -> Result<SealedGenerationCut, MemWalWorkerError> {
        let inflight = self.reserve_inflight_core()?;
        let registry = Arc::clone(self);
        crate::instrumentation::spawn_with_query_io_probes(async move {
            match registry
                .seal_and_drain_background(
                    key,
                    table_key,
                    authority,
                    opener,
                    inflight,
                    EmptyCutPolicy::Reject,
                )
                .await?
            {
                QuiesceCut::Generation(cut) => Ok(cut),
                QuiesceCut::Empty(_) => unreachable!("empty cuts are refused by seal_and_drain"),
            }
        })
        .await
        .map_err(|error| MemWalWorkerError::state(format!("seal task failed: {error}")))?
    }

    /// Retire any resident writer, claim a fresh writer under the same
    /// exclusive admission lease, and return its exact empty or generation
    /// cut. A resident writer is never reused: lifecycle quiescence must be
    /// proven against a fresh epoch claim after the old owner has settled.
    pub(crate) async fn quiesce_cut(
        self: &Arc<Self>,
        key: StreamWorkerKey,
        table_key: String,
        authority: CheckedExclusiveStreamAuthority,
        opener: WorkerOpener,
    ) -> Result<QuiesceCut, MemWalWorkerError> {
        let inflight = self.reserve_inflight_core()?;
        let registry = Arc::clone(self);
        crate::instrumentation::spawn_with_query_io_probes(async move {
            registry
                .quiesce_cut_background(key, table_key, authority, opener, inflight)
                .await
        })
        .await
        .map_err(|error| MemWalWorkerError::state(format!("quiesce task failed: {error}")))?
    }

    /// Install the writer claimed by an exact lifecycle resume while the
    /// caller still owns exclusive admission for the lane.
    ///
    /// Resume is unlike an ordinary cold put: its recovery owner publishes
    /// `OPEN` before there is a caller batch to drive the normal registry
    /// opener.  The claimed writer must nevertheless become registry-owned
    /// before the exclusive gate is released.  Any post-claim failure is
    /// transferred to the same exclusive retirement machinery used by
    /// quiescence, so no raw writer is dropped outside the registry.
    pub(crate) async fn install_resumed_writer(
        self: &Arc<Self>,
        key: StreamWorkerKey,
        table_key: String,
        authority: CheckedExclusiveStreamAuthority,
        opener: WorkerOpener,
        idle_authority: IdleAuthorityCheck,
    ) -> Result<(), MemWalWorkerError> {
        let inflight = self.reserve_inflight_core()?;
        let registry = Arc::clone(self);
        crate::instrumentation::spawn_with_query_io_probes(async move {
            registry
                .install_resumed_writer_background(
                    key,
                    table_key,
                    authority,
                    opener,
                    idle_authority,
                    inflight,
                )
                .await
        })
        .await
        .map_err(|error| MemWalWorkerError::state(format!("resume writer task failed: {error}")))?
    }

    async fn install_resumed_writer_background(
        self: Arc<Self>,
        key: StreamWorkerKey,
        table_key: String,
        authority: CheckedExclusiveStreamAuthority,
        opener: WorkerOpener,
        idle_authority: IdleAuthorityCheck,
        inflight: InFlightPermit,
    ) -> Result<(), MemWalWorkerError> {
        let slot = self.slot(key);
        let authority = self
            .retire_current_owner_for_quiesce(&slot, key, authority)
            .await?;
        let mut slot_state = slot.state.lock().await;
        if !matches!(&*slot_state, RegistrySlotState::Vacant) {
            return Err(MemWalWorkerError::state(
                "resume writer slot was not vacant after exclusive retirement",
            ));
        }
        self.reserve_resident(key.identity)?;
        let opening = Arc::new(tokio::sync::Notify::new());
        *slot_state = RegistrySlotState::Opening(Arc::clone(&opening));
        drop(slot_state);

        let opened = opener().await;
        let mut slot_state = slot.state.lock().await;
        match opened {
            Ok(opened) => {
                if !matches!(&*slot_state, RegistrySlotState::Opening(current) if Arc::ptr_eq(current, &opening))
                {
                    let fold_required = opened.fold_required_accounting();
                    let worker = Arc::new(MemWalWorker::from_opened(key, table_key, opened));
                    let install = self.install_worker_usage(&worker, fold_required);
                    *slot_state = RegistrySlotState::Active(Arc::clone(&worker));
                    opening.notify_waiters();
                    drop(slot_state);
                    *worker.mode.lock().await = WorkerMode::Retiring;
                    self.retain_exclusive_retirement(&slot, &worker, authority)
                        .await;
                    drop(inflight);
                    install?;
                    return Err(MemWalWorkerError::state(
                        "resume opening reservation changed; claimed writer is retiring",
                    ));
                }

                let refusal = opened.put_refusal(&table_key);
                let fold_required = opened.fold_required_accounting();
                let worker = Arc::new(MemWalWorker::from_opened(key, table_key, opened));
                *slot_state = RegistrySlotState::Active(Arc::clone(&worker));
                opening.notify_waiters();
                if let Err(error) = self.install_worker_usage(&worker, fold_required) {
                    drop(slot_state);
                    *worker.mode.lock().await = WorkerMode::Retiring;
                    self.retain_exclusive_retirement(&slot, &worker, authority)
                        .await;
                    drop(inflight);
                    return Err(error);
                }
                if let Some(error) = refusal {
                    drop(slot_state);
                    *worker.mode.lock().await = WorkerMode::Retiring;
                    self.retain_exclusive_retirement(&slot, &worker, authority)
                        .await;
                    drop(inflight);
                    return Err(MemWalWorkerError::state(format!(
                        "resume claimed a non-empty generation: {error}"
                    )));
                }
                drop(slot_state);
                self.ensure_idle_task(&slot, &worker, idle_authority);
                drop(authority);
                drop(inflight);
                Ok(())
            }
            Err(failure) => {
                let (error, claimed) = failure.into_parts();
                if let Some(claimed) = claimed {
                    let worker =
                        Arc::new(MemWalWorker::retiring_from_claimed(key, table_key, claimed));
                    let install = self.install_conservative_retiring_usage(&worker);
                    *slot_state = RegistrySlotState::Active(Arc::clone(&worker));
                    opening.notify_waiters();
                    drop(slot_state);
                    *worker.mode.lock().await = WorkerMode::Retiring;
                    self.retain_exclusive_retirement(&slot, &worker, authority)
                        .await;
                    drop(inflight);
                    install?;
                } else {
                    if matches!(&*slot_state, RegistrySlotState::Opening(current) if Arc::ptr_eq(current, &opening))
                    {
                        *slot_state = RegistrySlotState::Vacant;
                        opening.notify_waiters();
                        self.release_resident_reservation(key.identity);
                    }
                    drop(slot_state);
                    drop(authority);
                    drop(inflight);
                }
                Err(error)
            }
        }
    }

    /// Reconstruct a DRAINING cut without claiming a successor writer epoch.
    ///
    /// This is the restart path after a terminal drain claim has already been
    /// published. It first retires any process-local owner, then authenticates
    /// passive physical state against the manifest-selected current claim. An
    /// exact flushed generation can be reused directly; an exact empty
    /// projection can be sealed directly; a non-empty unsealed replay returns
    /// the still-held authority so the caller can run one unique fresh claim.
    pub(crate) async fn passive_quiesce_cut(
        self: &Arc<Self>,
        key: StreamWorkerKey,
        authority: CheckedExclusiveStreamAuthority,
        dataset: Dataset,
        draining: StreamLifecycleEntry,
        retained_inventory: Option<RetainedShardInventoryCommitment>,
        current_claim: ClaimReceipt,
    ) -> Result<PassiveQuiesceDisposition, MemWalWorkerError> {
        let inflight = self.reserve_inflight_core()?;
        let registry = Arc::clone(self);
        crate::instrumentation::spawn_with_query_io_probes(async move {
            registry
                .passive_quiesce_cut_background(
                    key,
                    authority,
                    dataset,
                    draining,
                    retained_inventory,
                    current_claim,
                    inflight,
                )
                .await
        })
        .await
        .map_err(|error| {
            MemWalWorkerError::state(format!("passive quiesce task failed: {error}"))
        })?
    }

    async fn passive_quiesce_cut_background(
        self: Arc<Self>,
        key: StreamWorkerKey,
        authority: CheckedExclusiveStreamAuthority,
        dataset: Dataset,
        draining: StreamLifecycleEntry,
        retained_inventory: Option<RetainedShardInventoryCommitment>,
        current_claim: ClaimReceipt,
        inflight: InFlightPermit,
    ) -> Result<PassiveQuiesceDisposition, MemWalWorkerError> {
        validate_passive_drain_claim(key, &draining, &current_claim)?;
        let slot = self.slot(key);
        let authority = self
            .retire_current_owner_for_quiesce(&slot, key, authority)
            .await?;
        let physical = validate_b1_lifecycle_physical_state_with_binding_inventory(
            &dataset,
            &draining,
            retained_inventory.as_ref(),
        )
        .await?;
        self.passive_disposition_from_physical(
            key,
            authority,
            &draining,
            &current_claim,
            physical,
            inflight,
        )
    }

    fn passive_disposition_from_physical(
        self: &Arc<Self>,
        key: StreamWorkerKey,
        authority: CheckedExclusiveStreamAuthority,
        draining: &StreamLifecycleEntry,
        current_claim: &ClaimReceipt,
        physical: PassiveB1PhysicalState,
        inflight: InFlightPermit,
    ) -> Result<PassiveQuiesceDisposition, MemWalWorkerError> {
        match physical {
            PassiveB1PhysicalState::FoldOnlyFlushed(flushed) => {
                validate_passive_flushed_claim_physical_cut(
                    current_claim,
                    flushed.shard_manifest_version,
                    flushed.writer_epoch,
                    flushed.replay_after_wal_entry_position,
                )?;
                let reservation = self.reserve_fold_opening_usage(key)?;
                let fold_usage = reservation.into_fold_usage(inflight);
                Ok(PassiveQuiesceDisposition::Reusable(QuiesceCut::Generation(
                    SealedGenerationCut {
                        key,
                        writer_epoch: flushed.writer_epoch,
                        shard_manifest_version: flushed.shard_manifest_version,
                        generation: flushed.generation,
                        path: flushed.path,
                        replay_after_wal_entry_position: flushed.replay_after_wal_entry_position,
                        _exclusive_authority: authority,
                        _fold_usage: fold_usage,
                    },
                )))
            }
            PassiveB1PhysicalState::AdmitOrReplay {
                shard_manifest_version,
                current_generation,
                replay_after_wal_entry_position,
                writer_epoch,
            } => {
                validate_passive_claim_epoch(current_claim, shard_manifest_version, writer_epoch)?;
                drop(inflight);
                let base_merged_generation =
                    current_generation.checked_sub(1).ok_or_else(|| {
                        MemWalWorkerError::state("passive empty successor has generation zero")
                    })?;
                if draining
                    .selected_claim_empty_cut_disposition(
                        current_claim,
                        shard_manifest_version,
                        writer_epoch,
                        replay_after_wal_entry_position,
                        current_generation,
                        base_merged_generation,
                    )
                    .is_some()
                {
                    Ok(PassiveQuiesceDisposition::Reusable(QuiesceCut::Empty(
                        EmptyFenceCut {
                            key,
                            writer_epoch,
                            shard_manifest_version,
                            current_generation,
                            replay_after_wal_entry_position,
                            _exclusive_authority: authority,
                        },
                    )))
                } else {
                    validate_passive_unflushed_claim_physical_cut(
                        current_claim,
                        shard_manifest_version,
                        writer_epoch,
                        replay_after_wal_entry_position,
                    )?;
                    if current_claim.proves_empty_current_generation() {
                        return Err(MemWalWorkerError::state(
                            "passive empty claim does not prove the observed generation topology",
                        ));
                    }
                    Ok(PassiveQuiesceDisposition::FreshClaimRequired(authority))
                }
            }
        }
    }

    async fn quiesce_cut_background(
        self: Arc<Self>,
        key: StreamWorkerKey,
        table_key: String,
        authority: CheckedExclusiveStreamAuthority,
        opener: WorkerOpener,
        inflight: InFlightPermit,
    ) -> Result<QuiesceCut, MemWalWorkerError> {
        let slot = self.slot(key);
        let authority = self
            .retire_current_owner_for_quiesce(&slot, key, authority)
            .await?;
        self.seal_and_drain_background(
            key,
            table_key,
            authority,
            opener,
            inflight,
            EmptyCutPolicy::ReturnFence,
        )
        .await
    }

    async fn retire_current_owner_for_quiesce(
        self: &Arc<Self>,
        slot: &Arc<RegistrySlot>,
        key: StreamWorkerKey,
        authority: CheckedExclusiveStreamAuthority,
    ) -> Result<CheckedExclusiveStreamAuthority, MemWalWorkerError> {
        let worker = {
            let state = slot.state.lock().await;
            match &*state {
                RegistrySlotState::Vacant => return Ok(authority),
                RegistrySlotState::Opening(_) => {
                    return Err(MemWalWorkerError::state(
                        "cannot quiesce while a cold stream claim is in progress",
                    ));
                }
                RegistrySlotState::Active(worker) => Arc::clone(worker),
                RegistrySlotState::Retiring(_) => {
                    return Err(MemWalWorkerError::Retiring {
                        key,
                        reason: "prior retirement has not completed".to_string(),
                    });
                }
            }
        };

        *worker.mode.lock().await = WorkerMode::Retiring;
        let retirement = self.begin_abort(slot, &worker).await;
        match tokio::time::timeout(self.limits.abort_deadline, retirement.clone().wait()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                self.retain_existing_exclusive_retirement(
                    slot,
                    Arc::clone(&worker),
                    retirement,
                    authority,
                );
                return Err(error);
            }
            Err(_) => {
                self.retain_existing_exclusive_retirement(
                    slot,
                    Arc::clone(&worker),
                    retirement,
                    authority,
                );
                return Err(MemWalWorkerError::state(format!(
                    "resident writer abort did not settle within {} ms before fresh quiesce claim",
                    self.limits.abort_deadline.as_millis()
                )));
            }
        }

        let mut state = slot.state.lock().await;
        if !matches!(&*state, RegistrySlotState::Retiring(_)) {
            drop(state);
            self.release_worker_usage(&worker);
            return Err(MemWalWorkerError::state(
                "resident writer slot changed after quiesce retirement",
            ));
        }
        *state = RegistrySlotState::Vacant;
        drop(state);
        self.release_worker_usage(&worker);
        Ok(authority)
    }

    async fn seal_and_drain_background(
        self: Arc<Self>,
        key: StreamWorkerKey,
        table_key: String,
        authority: CheckedExclusiveStreamAuthority,
        opener: WorkerOpener,
        inflight: InFlightPermit,
        empty_cut_policy: EmptyCutPolicy,
    ) -> Result<QuiesceCut, MemWalWorkerError> {
        let seal_deadline = self.limits.seal_deadline;
        let deadline = tokio::time::Instant::now() + seal_deadline;
        let slot = self.slot(key);
        let mut slot_state = slot.state.lock().await;
        let worker = match &*slot_state {
            RegistrySlotState::Vacant => {
                let mut opening_usage = self.reserve_fold_opening_usage(key)?;
                let opening = Arc::new(tokio::sync::Notify::new());
                *slot_state = RegistrySlotState::Opening(Arc::clone(&opening));
                drop(slot_state);
                let mut open_task = crate::instrumentation::spawn_with_query_io_probes(opener());
                let opened = match tokio::time::timeout_at(deadline, &mut open_task).await {
                    Ok(Ok(result)) => result,
                    Ok(Err(error)) => {
                        self.retain_failed_fold_open_join(authority, inflight, opening_usage);
                        return Err(MemWalWorkerError::state(format!(
                            "cold fold opener owner task failed after a possible writer claim: {error}"
                        )));
                    }
                    Err(_) => {
                        self.retain_fold_open_after_timeout(
                            Arc::clone(&slot),
                            key,
                            table_key,
                            Arc::clone(&opening),
                            opening_usage,
                            authority,
                            inflight,
                            open_task,
                        );
                        return Err(MemWalWorkerError::state(format!(
                            "cold fold opener did not settle within {} ms; its owned continuation retains exclusive authority and the full generation reservation",
                            seal_deadline.as_millis()
                        )));
                    }
                };
                let opened = match opened {
                    Ok(opened) => opened,
                    Err(failure) => {
                        let (error, claimed) = failure.into_parts();
                        let mut slot_state = slot.state.lock().await;
                        if let Some(claimed) = claimed {
                            let worker = Arc::new(MemWalWorker::retiring_from_claimed(
                                key, table_key, claimed,
                            ));
                            if let Err(transfer_error) = opening_usage
                                .transfer_to_worker(&worker, full_generation_reservation())
                            {
                                self.retain_failed_fold_open_transfer(
                                    authority,
                                    inflight,
                                    opening_usage,
                                    worker,
                                );
                                return Err(MemWalWorkerError::state(format!(
                                    "cold fold claimed a writer but its full opening reservation could not transfer; ownership is retained: {transfer_error}"
                                )));
                            }
                            *slot_state = RegistrySlotState::Active(Arc::clone(&worker));
                            opening.notify_waiters();
                            drop(slot_state);
                            self.retain_exclusive_retirement(&slot, &worker, authority)
                                .await;
                        } else if matches!(&*slot_state, RegistrySlotState::Opening(current) if Arc::ptr_eq(current, &opening))
                        {
                            *slot_state = RegistrySlotState::Vacant;
                            opening.notify_waiters();
                        }
                        return Err(error);
                    }
                };
                slot_state = slot.state.lock().await;
                if !matches!(&*slot_state, RegistrySlotState::Opening(current) if Arc::ptr_eq(current, &opening))
                {
                    let fold_required = opened
                        .fold_required_accounting()
                        .unwrap_or_else(full_generation_reservation);
                    let worker = Arc::new(MemWalWorker::from_opened(key, table_key, opened));
                    if let Err(transfer_error) =
                        opening_usage.transfer_to_worker(&worker, fold_required)
                    {
                        self.retain_failed_fold_open_transfer(
                            authority,
                            inflight,
                            opening_usage,
                            worker,
                        );
                        return Err(MemWalWorkerError::state(format!(
                            "cold fold opened a writer after its slot changed but its reservation could not transfer; ownership is retained: {transfer_error}"
                        )));
                    }
                    *slot_state = RegistrySlotState::Active(Arc::clone(&worker));
                    opening.notify_waiters();
                    drop(slot_state);
                    *worker.mode.lock().await = WorkerMode::Retiring;
                    self.retain_exclusive_retirement(&slot, &worker, authority)
                        .await;
                    return Err(MemWalWorkerError::state(
                        "fold opening reservation changed during claim; claimed writer is retiring",
                    ));
                }
                let fold_required = opened
                    .fold_required_accounting()
                    .unwrap_or_else(full_generation_reservation);
                let worker = Arc::new(MemWalWorker::from_opened(key, table_key, opened));
                if let Err(transfer_error) =
                    opening_usage.transfer_to_worker(&worker, fold_required)
                {
                    self.retain_failed_fold_open_transfer(
                        authority,
                        inflight,
                        opening_usage,
                        worker,
                    );
                    return Err(MemWalWorkerError::state(format!(
                        "cold fold opened a writer but its reservation could not transfer; ownership is retained: {transfer_error}"
                    )));
                }
                *slot_state = RegistrySlotState::Active(Arc::clone(&worker));
                opening.notify_waiters();
                worker
            }
            RegistrySlotState::Opening(_) => {
                return Err(MemWalWorkerError::state(
                    "a cold stream claim is in progress; retry fold after it settles",
                ));
            }
            RegistrySlotState::Active(worker) => Arc::clone(worker),
            RegistrySlotState::Retiring(_) => {
                return Err(MemWalWorkerError::Retiring {
                    key,
                    reason: "prior retirement has not completed".to_string(),
                });
            }
        };
        drop(slot_state);
        let mut mode = worker.mode.lock().await;
        enum PhysicalSealPlan {
            SealActive(Option<ReplayGenerationState>),
            ProveFlushed(FlushedGenerationState),
        }
        if matches!(&*mode, WorkerMode::Admit(accounting) if accounting.batches == 0) {
            *mode = WorkerMode::Retiring;
            drop(mode);
            if matches!(empty_cut_policy, EmptyCutPolicy::Reject) {
                self.retain_exclusive_retirement(&slot, &worker, authority)
                    .await;
                return Err(MemWalWorkerError::state(
                    "cannot seal an empty admitted generation; empty claimed writer is retiring",
                ));
            }

            let proof_worker = Arc::clone(&worker);
            let mut physical_cut =
                tokio::spawn(async move { prove_empty_fence_cut(&proof_worker.writer).await });
            let cut = match tokio::time::timeout_at(deadline, &mut physical_cut).await {
                Ok(Ok(Ok(cut))) => cut,
                Ok(Ok(Err(error))) => {
                    self.retain_exclusive_retirement(&slot, &worker, authority)
                        .await;
                    return Err(error);
                }
                Ok(Err(error)) => {
                    self.retain_exclusive_retirement(&slot, &worker, authority)
                        .await;
                    return Err(MemWalWorkerError::state(format!(
                        "empty-cut proof owner task failed: {error}"
                    )));
                }
                Err(_) => {
                    self.retain_exclusive_after_settle(&slot, &worker, authority, async move {
                        let _ = physical_cut.await;
                    });
                    return Err(MemWalWorkerError::state(format!(
                        "empty-cut proof did not settle within {} ms; fence outcome is ambiguous",
                        seal_deadline.as_millis()
                    )));
                }
            };

            let retirement = self.begin_abort(&slot, &worker).await;
            let abort_deadline =
                deadline.min(tokio::time::Instant::now() + self.limits.abort_deadline);
            match tokio::time::timeout_at(abort_deadline, retirement.clone().wait()).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    self.retain_existing_exclusive_retirement(
                        &slot,
                        Arc::clone(&worker),
                        retirement,
                        authority,
                    );
                    return Err(error);
                }
                Err(_) => {
                    self.retain_existing_exclusive_retirement(
                        &slot,
                        Arc::clone(&worker),
                        retirement,
                        authority,
                    );
                    return Err(MemWalWorkerError::state(format!(
                        "empty writer abort did not settle within the {} ms seal deadline",
                        seal_deadline.as_millis()
                    )));
                }
            }
            let mut slot_state = slot.state.lock().await;
            *slot_state = RegistrySlotState::Vacant;
            drop(slot_state);
            self.release_worker_usage(&worker);
            drop(inflight);
            return Ok(QuiesceCut::Empty(EmptyFenceCut {
                key,
                writer_epoch: worker.writer.epoch(),
                shard_manifest_version: cut.shard_manifest_version,
                current_generation: cut.current_generation,
                replay_after_wal_entry_position: cut.replay_after_wal_entry_position,
                _exclusive_authority: authority,
            }));
        }
        let plan = match &*mode {
            WorkerMode::Admit(_) => PhysicalSealPlan::SealActive(None),
            WorkerMode::FoldReplay(replay) => PhysicalSealPlan::SealActive(Some(replay.clone())),
            WorkerMode::FoldFlushed(flushed) => PhysicalSealPlan::ProveFlushed(flushed.clone()),
            WorkerMode::Retiring => {
                return Err(MemWalWorkerError::Retiring {
                    key,
                    reason: "worker is already retiring".to_string(),
                });
            }
        };
        // Reserve the widest legal fresh scan before the first physical seal
        // effect. This is deliberately conservative for already-flushed
        // restart state, whose resident Arrow charge is otherwise zero.
        self.reserve_fold_usage(&worker)?;
        // The physical cut is now background-owned.  Mark the worker closed
        // to new local work before releasing the mode mutex, then let the
        // owner run bridge/seal/drain/proof to a settled result even if the
        // deadline below expires.
        *mode = WorkerMode::Retiring;
        drop(mode);
        let physical_worker = Arc::clone(&worker);
        let mut physical_cut = tokio::spawn(async move {
            let cut = match plan {
                PhysicalSealPlan::ProveFlushed(flushed) => {
                    prove_existing_flushed_cut(&physical_worker.writer, &flushed).await?
                }
                PhysicalSealPlan::SealActive(replay) => {
                    if let Some(replay) = replay.as_ref() {
                        bridge_replayed_batch_store_watermark(&physical_worker.writer, replay)
                            .await?;
                    }
                    let before = physical_worker
                        .writer
                        .in_memory_memtable_refs()
                        .await
                        .map_err(|error| MemWalWorkerError::lance("pre-seal snapshot", error))?;
                    if !before.frozen.is_empty() || before.active.batch_store.iter().len() == 0 {
                        return Err(MemWalWorkerError::state(
                            "pre-seal snapshot is not one non-empty active generation",
                        ));
                    }
                    let expected_generation = before.active.generation;
                    let expected_cursor = physical_worker
                        .writer
                        .wal_stats()
                        .next_wal_entry_position
                        .saturating_sub(1);
                    crate::failpoints::maybe_fail(
                        crate::failpoints::names::STREAM_B1_BEFORE_FORCE_SEAL,
                    )
                    .map_err(|error| MemWalWorkerError::state(error.to_string()))?;
                    physical_worker
                        .writer
                        .force_seal_active()
                        .await
                        .map_err(|error| MemWalWorkerError::lance("force seal", error))?;
                    crate::failpoints::maybe_fail(
                        crate::failpoints::names::STREAM_B1_AFTER_FORCE_SEAL,
                    )
                    .map_err(|error| MemWalWorkerError::state(error.to_string()))?;
                    physical_worker
                        .writer
                        .wait_for_flush_drain()
                        .await
                        .map_err(|error| MemWalWorkerError::lance("flush drain", error))?;
                    crate::failpoints::maybe_fail(
                        crate::failpoints::names::STREAM_B1_AFTER_FLUSH_DRAIN_BEFORE_PROOF,
                    )
                    .map_err(|error| MemWalWorkerError::state(error.to_string()))?;
                    prove_post_drain_cut(
                        &physical_worker.writer,
                        expected_generation,
                        Some(expected_cursor),
                    )
                    .await?
                }
            };
            crate::failpoints::maybe_fail(
                crate::failpoints::names::STREAM_B1_AFTER_POST_DRAIN_PROOF,
            )
            .map_err(|error| MemWalWorkerError::state(error.to_string()))?;
            Ok(cut)
        });
        let cut = match tokio::time::timeout_at(deadline, &mut physical_cut).await {
            Ok(Ok(Ok(cut))) => cut,
            Ok(Ok(Err(error))) => {
                self.retain_exclusive_retirement(&slot, &worker, authority)
                    .await;
                return Err(error);
            }
            Ok(Err(error)) => {
                self.retain_exclusive_retirement(&slot, &worker, authority)
                    .await;
                return Err(MemWalWorkerError::state(format!(
                    "seal/drain owner task failed: {error}"
                )));
            }
            Err(_) => {
                self.retain_exclusive_after_settle(&slot, &worker, authority, async move {
                    let _ = physical_cut.await;
                });
                return Err(MemWalWorkerError::state(format!(
                    "seal/drain did not settle within {} ms; generation outcome is ambiguous",
                    seal_deadline.as_millis()
                )));
            }
        };

        let retirement = self.begin_abort(&slot, &worker).await;
        let abort_deadline = deadline.min(tokio::time::Instant::now() + self.limits.abort_deadline);
        match tokio::time::timeout_at(abort_deadline, retirement.clone().wait()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                self.retain_existing_exclusive_retirement(
                    &slot,
                    Arc::clone(&worker),
                    retirement,
                    authority,
                );
                return Err(error);
            }
            Err(_) => {
                self.retain_existing_exclusive_retirement(
                    &slot,
                    Arc::clone(&worker),
                    retirement,
                    authority,
                );
                return Err(MemWalWorkerError::state(format!(
                    "writer abort did not settle within the {} ms seal deadline",
                    seal_deadline.as_millis()
                )));
            }
        }
        let mut slot_state = slot.state.lock().await;
        let usage = self.take_worker_usage(&worker);
        debug_assert!(usage.resident);
        debug_assert_eq!(usage.accounting.arrow_bytes, B1_MAX_GENERATION_ARROW_BYTES);
        debug_assert_eq!(usage.pending_generations, 1);
        let fold_usage = FoldUsagePermit {
            registry: Arc::clone(&self),
            key: worker.key,
            usage,
            _inflight: inflight,
        };
        *slot_state = RegistrySlotState::Vacant;
        drop(slot_state);
        Ok(QuiesceCut::Generation(SealedGenerationCut {
            key,
            writer_epoch: worker.writer.epoch(),
            shard_manifest_version: cut.shard_manifest_version,
            generation: cut.generation,
            path: cut.path,
            replay_after_wal_entry_position: cut.replay_after_wal_entry_position,
            _exclusive_authority: authority,
            _fold_usage: fold_usage,
        }))
    }

    async fn retain_exclusive_retirement(
        self: &Arc<Self>,
        slot: &Arc<RegistrySlot>,
        worker: &Arc<MemWalWorker>,
        authority: CheckedExclusiveStreamAuthority,
    ) {
        let retirement = self.begin_abort(slot, worker).await;
        self.retain_existing_exclusive_retirement(slot, Arc::clone(worker), retirement, authority);
    }

    /// Exclusive counterpart of `retain_shared_after_settle`: a seal/drain
    /// deadline detaches this owner, which keeps the exclusive cut authority
    /// until the already-invoked physical operation settles and only then
    /// starts retirement.
    fn retain_exclusive_after_settle<F>(
        self: &Arc<Self>,
        slot: &Arc<RegistrySlot>,
        worker: &Arc<MemWalWorker>,
        authority: CheckedExclusiveStreamAuthority,
        settle: F,
    ) where
        F: Future<Output = ()> + Send + 'static,
    {
        let registry = Arc::clone(self);
        let slot = Arc::clone(slot);
        let worker = Arc::clone(worker);
        spawn_after_settle(authority, settle, move |authority| async move {
            registry
                .retain_exclusive_retirement(&slot, &worker, authority)
                .await;
        });
    }

    fn retain_existing_exclusive_retirement(
        self: &Arc<Self>,
        slot: &Arc<RegistrySlot>,
        worker: Arc<MemWalWorker>,
        retirement: RetirementHandle,
        authority: CheckedExclusiveStreamAuthority,
    ) {
        let slot = Arc::clone(slot);
        let registry = Arc::clone(self);
        tokio::spawn(async move {
            let _authority = authority;
            let completed = retirement.wait().await.is_ok();
            if completed {
                let mut state = slot.state.lock().await;
                if matches!(&*state, RegistrySlotState::Retiring(_)) {
                    *state = RegistrySlotState::Vacant;
                }
                drop(state);
                registry.release_worker_usage(&worker);
                drop(_authority);
                let _ = crate::failpoints::maybe_fail(
                    crate::failpoints::names::STREAM_B1_AFTER_RETIREMENT_RELEASE,
                );
            } else {
                // A failed abort is fail-closed for the rest of this process.
                std::future::pending::<()>().await;
            }
        });
    }
}

fn validate_passive_drain_claim(
    key: StreamWorkerKey,
    draining: &StreamLifecycleEntry,
    current_claim: &ClaimReceipt,
) -> Result<(), MemWalWorkerError> {
    draining
        .validate()
        .map_err(|error| MemWalWorkerError::config(error.to_string()))?;
    current_claim
        .validate()
        .map_err(|error| MemWalWorkerError::config(error.to_string()))?;
    let drain = draining.drain.as_ref().ok_or_else(|| {
        MemWalWorkerError::state("passive quiesce requires an exact DRAINING lifecycle")
    })?;
    let physical_binding_digest = stream_physical_binding_digest(&draining.binding)
        .map_err(|error| MemWalWorkerError::config(error.to_string()))?;
    let expected_claim_chain = current_claim
        .next_chain_ref()
        .map_err(|error| MemWalWorkerError::config(error.to_string()))?;
    let epoch_floor = draining
        .epoch_floor_by_shard
        .get(&key.shard_id.to_string())
        .copied()
        .ok_or_else(|| {
            MemWalWorkerError::state("passive quiesce lifecycle omits the bound shard epoch")
        })?;
    let drain_target_epoch = drain
        .target_epoch_floor_by_shard
        .get(&key.shard_id.to_string())
        .copied()
        .ok_or_else(|| {
            MemWalWorkerError::state("passive quiesce drain omits the bound shard epoch target")
        })?;
    if draining.lifecycle != StreamLifecycle::Draining
        || draining.strict_block.is_some()
        || draining.identity != key.identity
        || draining.binding.enrollment_id != key.enrollment_id.to_string()
        || draining.binding.shard_ids != vec![key.shard_id.to_string()]
        || current_claim.identity != key.identity
        || current_claim.lifecycle_operation_id.as_deref() != Some(drain.drain_id.as_str())
        || current_claim.binding_scope_id != draining.binding_scope_id
        || current_claim.enrollment_id != draining.binding.enrollment_id
        || current_claim.shard_id != key.shard_id.to_string()
        || current_claim.stream_incarnation_id != draining.enrollment_receipt.stream_incarnation_id
        || current_claim.stream_configuration_digest != draining.binding.stream_config_hash
        || current_claim.physical_binding_digest != physical_binding_digest
        || draining.current_claim_receipt_id.as_deref() != Some(current_claim.record_id.as_str())
        || expected_claim_chain != draining.claim_receipt_chain
        || current_claim.authenticated_tail_position != draining.authenticated_wal_tail.position
        || current_claim.authenticated_tail_segment_count
            != draining.authenticated_wal_tail.segment_count
        || current_claim.authenticated_tail_chain_digest
            != draining.authenticated_wal_tail.chain_digest
        || current_claim.authenticated_tail_lww_projection_digest
            != draining.authenticated_wal_tail.lww_projection_digest
        || current_claim.sentinel_position != draining.authenticated_wal_tail.position
        || epoch_floor != current_claim.achieved_writer_epoch
        || drain_target_epoch != current_claim.achieved_writer_epoch
    {
        return Err(MemWalWorkerError::state(
            "passive quiesce claim differs from current drain, binding, receipt-chain, lifecycle/drain epoch, sentinel, cursor, or authenticated-tail authority",
        ));
    }
    Ok(())
}

fn validate_passive_claim_epoch(
    current_claim: &ClaimReceipt,
    shard_manifest_version: u64,
    writer_epoch: u64,
) -> Result<(), MemWalWorkerError> {
    if shard_manifest_version < current_claim.achieved_shard_manifest_version
        || writer_epoch != current_claim.achieved_writer_epoch
    {
        return Err(MemWalWorkerError::state(format!(
            "passive quiesce physical cut (manifest={shard_manifest_version}, epoch={writer_epoch}) differs from current claim (manifest={}, epoch={})",
            current_claim.achieved_shard_manifest_version, current_claim.achieved_writer_epoch,
        )));
    }
    Ok(())
}

fn validate_passive_flushed_claim_physical_cut(
    current_claim: &ClaimReceipt,
    shard_manifest_version: u64,
    writer_epoch: u64,
    replay_after_wal_entry_position: u64,
) -> Result<(), MemWalWorkerError> {
    validate_passive_claim_epoch(current_claim, shard_manifest_version, writer_epoch)?;
    if replay_after_wal_entry_position != current_claim.sentinel_position {
        return Err(MemWalWorkerError::state(format!(
            "passive flushed quiesce cursor {replay_after_wal_entry_position} differs from current claim sentinel {}",
            current_claim.sentinel_position,
        )));
    }
    Ok(())
}

fn validate_passive_unflushed_claim_physical_cut(
    current_claim: &ClaimReceipt,
    shard_manifest_version: u64,
    writer_epoch: u64,
    replay_after_wal_entry_position: u64,
) -> Result<(), MemWalWorkerError> {
    validate_passive_claim_epoch(current_claim, shard_manifest_version, writer_epoch)?;
    if replay_after_wal_entry_position != current_claim.replay_cursor {
        return Err(MemWalWorkerError::state(format!(
            "passive unflushed quiesce cursor {replay_after_wal_entry_position} differs from current claim replay cursor {}",
            current_claim.replay_cursor,
        )));
    }
    Ok(())
}

fn key_identity(key: StreamWorkerKey) -> TableIdentity {
    key.identity
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProvenCut {
    shard_manifest_version: u64,
    generation: u64,
    path: String,
    replay_after_wal_entry_position: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProvenEmptyCut {
    shard_manifest_version: u64,
    current_generation: u64,
    replay_after_wal_entry_position: u64,
}

async fn prove_empty_fence_cut(writer: &ShardWriter) -> Result<ProvenEmptyCut, MemWalWorkerError> {
    writer
        .check_fenced()
        .await
        .map_err(|error| MemWalWorkerError::lance("empty-cut writer fence check", error))?;
    let in_memory = writer
        .in_memory_memtable_refs()
        .await
        .map_err(|error| MemWalWorkerError::lance("empty-cut in-memory proof", error))?;
    if !in_memory.frozen.is_empty() || in_memory.active.batch_store.iter().len() != 0 {
        return Err(MemWalWorkerError::state(
            "empty-cut in-memory state is not one empty active generation",
        ));
    }
    let manifest = writer
        .manifest()
        .await
        .map_err(|error| MemWalWorkerError::lance("empty-cut manifest proof", error))?
        .ok_or_else(|| MemWalWorkerError::state("empty-cut shard manifest is absent"))?;
    if manifest.shard_id != writer.shard_id()
        || manifest.shard_spec_id != UNSHARDED_SPEC_ID
        || manifest.status != ShardStatus::Active
        || manifest.writer_epoch != writer.epoch()
        || manifest.current_generation != in_memory.active.generation
    {
        return Err(MemWalWorkerError::state(format!(
            "empty-cut manifest does not prove the freshly claimed empty writer: {manifest:?}"
        )));
    }
    Ok(ProvenEmptyCut {
        shard_manifest_version: manifest.version,
        current_generation: manifest.current_generation,
        replay_after_wal_entry_position: manifest.replay_after_wal_entry_position,
    })
}

async fn prove_existing_flushed_cut(
    writer: &ShardWriter,
    expected: &FlushedGenerationState,
) -> Result<ProvenCut, MemWalWorkerError> {
    prove_post_drain_cut(
        writer,
        expected.generation,
        Some(expected.replay_after_wal_entry_position),
    )
    .await
    .and_then(|cut| {
        if cut.path == expected.path
            // Lance may publish later bookkeeping versions for the same
            // claimed epoch after the flushed cut was first classified. The
            // generation, path, cursor, and writer epoch are the authority;
            // the manifest version must only be monotonic.
            && cut.shard_manifest_version >= expected.shard_manifest_version
            && writer.epoch() == expected.writer_epoch
        {
            Ok(cut)
        } else {
            Err(MemWalWorkerError::state(format!(
                "flushed generation proof changed: expected={expected:?}, observed={cut:?}, writer_epoch={}",
                writer.epoch()
            )))
        }
    })
}

async fn prove_post_drain_cut(
    writer: &ShardWriter,
    expected_generation: u64,
    expected_cursor: Option<u64>,
) -> Result<ProvenCut, MemWalWorkerError> {
    let in_memory = writer
        .in_memory_memtable_refs()
        .await
        .map_err(|error| MemWalWorkerError::lance("post-drain in-memory proof", error))?;
    if !in_memory.frozen.is_empty()
        || in_memory.active.generation != expected_generation.saturating_add(1)
        || in_memory.active.batch_store.iter().len() != 0
    {
        return Err(MemWalWorkerError::state(format!(
            "post-drain in-memory state is not empty successor generation {}",
            expected_generation.saturating_add(1)
        )));
    }
    let manifest = writer
        .manifest()
        .await
        .map_err(|error| MemWalWorkerError::lance("post-drain manifest proof", error))?
        .ok_or_else(|| MemWalWorkerError::state("post-drain shard manifest is absent"))?;
    if manifest.writer_epoch != writer.epoch()
        || manifest.current_generation != expected_generation.saturating_add(1)
        || expected_cursor
            .is_some_and(|expected| manifest.replay_after_wal_entry_position != expected)
    {
        return Err(MemWalWorkerError::state(format!(
            "post-drain manifest does not prove generation {expected_generation} at comparator cursor {expected_cursor:?}: {manifest:?}"
        )));
    }
    let matches = manifest
        .flushed_generations
        .iter()
        .filter(|generation| generation.generation == expected_generation)
        .collect::<Vec<&FlushedGeneration>>();
    let [generation] = matches.as_slice() else {
        return Err(MemWalWorkerError::state(format!(
            "post-drain manifest has {} entries for generation {expected_generation}",
            matches.len()
        )));
    };
    Ok(ProvenCut {
        shard_manifest_version: manifest.version,
        generation: generation.generation,
        path: generation.path.clone(),
        replay_after_wal_entry_position: manifest.replay_after_wal_entry_position,
    })
}

/// A physical quiescence cut obtained after retiring any cached writer under
/// exclusive admission. Ordinary quiescence makes a fresh epoch claim;
/// restart recovery may instead authenticate an already-published claim and
/// reuse its exact passive cut.
pub(crate) enum QuiesceCut {
    Empty(EmptyFenceCut),
    Generation(SealedGenerationCut),
}

/// Restart-time outcome of passive DRAINING inspection.
///
/// The fresh-claim branch returns the exclusive token explicitly, preventing
/// the caller from accidentally opening an admission window between passive
/// proof and its unique successor claim.
pub(crate) enum PassiveQuiesceDisposition {
    Reusable(QuiesceCut),
    FreshClaimRequired(CheckedExclusiveStreamAuthority),
}

impl QuiesceCut {
    pub(crate) fn into_exclusive_authority(self) -> CheckedExclusiveStreamAuthority {
        match self {
            Self::Empty(cut) => cut.into_exclusive_authority(),
            Self::Generation(cut) => cut.into_exclusive_authority(),
        }
    }
}

/// Exact proof that a freshly claimed writer owned one empty active
/// generation before it was retired. The exclusive admission lease remains
/// held so graph orchestration can publish the lifecycle transition without
/// reopening the stream.
pub(crate) struct EmptyFenceCut {
    pub(crate) key: StreamWorkerKey,
    pub(crate) writer_epoch: u64,
    pub(crate) shard_manifest_version: u64,
    pub(crate) current_generation: u64,
    pub(crate) replay_after_wal_entry_position: u64,
    _exclusive_authority: CheckedExclusiveStreamAuthority,
}

impl EmptyFenceCut {
    pub(crate) fn into_exclusive_authority(self) -> CheckedExclusiveStreamAuthority {
        self._exclusive_authority
    }
}

/// Immutable fresh-tier cut plus the still-held exclusive admission lease.
pub(crate) struct SealedGenerationCut {
    pub(crate) key: StreamWorkerKey,
    pub(crate) writer_epoch: u64,
    pub(crate) shard_manifest_version: u64,
    pub(crate) generation: u64,
    pub(crate) path: String,
    pub(crate) replay_after_wal_entry_position: u64,
    _exclusive_authority: CheckedExclusiveStreamAuthority,
    _fold_usage: FoldUsagePermit,
}

impl SealedGenerationCut {
    pub(crate) fn fresh_only_snapshot(&self) -> lance::dataset::mem_wal::ShardSnapshot {
        lance::dataset::mem_wal::ShardSnapshot {
            shard_id: self.key.shard_id,
            spec_id: UNSHARDED_SPEC_ID,
            current_generation: self.generation.saturating_add(1),
            flushed_generations: vec![lance::dataset::mem_wal::scanner::FlushedGeneration {
                generation: self.generation,
                path: self.path.clone(),
            }],
        }
    }

    pub(crate) fn into_exclusive_authority(self) -> CheckedExclusiveStreamAuthority {
        let Self {
            _exclusive_authority,
            _fold_usage,
            ..
        } = self;
        drop(_fold_usage);
        _exclusive_authority
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::AtomicUsize;

    use arrow_array::{Int32Array, RecordBatchIterator, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use lance::dataset::mem_wal::DatasetMemWalExt;
    // forbidden-api-allow: test-only raw Lance fixture mutation and reopen
    use lance::dataset::{MergeInsertBuilder, WhenMatched, WhenNotMatched, WriteMode, WriteParams};
    use lance::datatypes::LANCE_UNENFORCED_PRIMARY_KEY;
    use lance_file::version::LanceFileVersion;
    use lance_index::mem_wal::{MergedGeneration, ShardingField, ShardingSpec};

    use super::super::{
        MemWalEnrollmentPlan, capture_pre_enrollment_head, initialize_index_from_exact_no_effect,
        provision_shard_from_exact_index_only,
    };
    use super::*;

    fn test_digest(nibble: char) -> String {
        format!("sha256:{}", nibble.to_string().repeat(64))
    }

    fn passive_test_drain_and_claim(
        key: StreamWorkerKey,
        achieved_shard_manifest_version: u64,
        achieved_writer_epoch: u64,
        sentinel_position: u64,
        sentinel_only: bool,
    ) -> (StreamLifecycleEntry, ClaimReceipt) {
        use crate::db::manifest::stream::{
            AuthenticatedWalTail, BindingReceipt, ClaimProfile, ClaimReceiptPreimage,
            ClaimTerminalClassification, CurrentHeadWitness, DrainDescriptor, DrainGoal,
            EnrollmentReceipt, QUIESCE_REQUEST_PROTOCOL_VERSION, QuiesceRequestPayload,
            authenticated_wal_tail_chain_digest, stream_graph_identity_digest,
        };
        use lance::dataset::refs::BranchIdentifier;

        assert!(achieved_shard_manifest_version > 1);
        assert!(achieved_writer_epoch > 1);
        assert!(sentinel_position > 0);
        let graph_identity_digest =
            stream_graph_identity_digest("passive-worker-test-domain").unwrap();
        let enrollment_request_id = ShardId::new_v4().to_string();
        let stream_incarnation_id = ShardId::new_v4().to_string();
        let binding_scope_id = ShardId::new_v4().to_string();
        let binding = StreamPhysicalBinding {
            stable_table_id: key.identity.stable_table_id,
            table_incarnation_id: key.identity.table_incarnation_id,
            table_location: format!(
                "nodes/{}-{}",
                key.identity.stable_table_id, key.identity.table_incarnation_id
            ),
            table_branch: None,
            enrollment_id: key.enrollment_id.to_string(),
            shard_ids: vec![key.shard_id.to_string()],
            stream_config_version: STREAM_CONFIG_VERSION,
            stream_config_hash: stream_config_v3_hash(),
        };
        let enrollment_receipt = EnrollmentReceipt::new(
            enrollment_request_id,
            test_digest('1'),
            stream_incarnation_id.clone(),
            binding.clone(),
        )
        .unwrap();
        let binding_receipt = BindingReceipt::new(
            graph_identity_digest.clone(),
            key.identity,
            &crate::db::manifest::stream::test_initial_binding_prior_chain(),
            &binding_scope_id,
            &stream_incarnation_id,
            binding.clone(),
            "INITIAL_ENROLLMENT",
            1,
        )
        .unwrap();
        let initial_epoch = achieved_writer_epoch - 1;
        let open = StreamLifecycleEntry::new_open_enrollment(
            key.identity,
            "node:PassiveTest".to_string(),
            binding.clone(),
            binding_scope_id.clone(),
            CurrentHeadWitness {
                branch_identifier: BranchIdentifier::main(),
                table_version: 1,
                transaction_uuid: ShardId::new_v4().to_string(),
                manifest_e_tag: None,
            },
            BTreeMap::from([(key.shard_id.to_string(), initial_epoch)]),
            enrollment_receipt,
            binding_receipt.record_id.clone(),
            binding_receipt.next_chain_ref().unwrap(),
        )
        .unwrap();
        let drain_id = ShardId::new_v4().to_string();
        let mut draining = open.clone();
        draining.lifecycle = StreamLifecycle::Draining;
        draining.lifecycle_revision += 1;
        let operation_request_payload = QuiesceRequestPayload {
            protocol_version: QUIESCE_REQUEST_PROTOCOL_VERSION,
            graph_identity_digest: graph_identity_digest.clone(),
            identity: key.identity,
            stream_incarnation_id: stream_incarnation_id.clone(),
            binding_scope_id: binding_scope_id.clone(),
            enrollment_id: binding.enrollment_id.clone(),
            drain_id: drain_id.clone(),
            expected_lifecycle_revision: open.lifecycle_revision,
            goal: DrainGoal::Sealed,
            physical_binding_digest: stream_physical_binding_digest(&binding).unwrap(),
            expected_current_head_witness: open.current_head_witness.clone(),
            target_epoch_floor_by_shard: BTreeMap::from([(
                key.shard_id.to_string(),
                initial_epoch,
            )]),
            seal_override: None,
        };
        let operation_request_digest = operation_request_payload.request_digest().unwrap();
        draining.drain = Some(DrainDescriptor {
            drain_id: drain_id.clone(),
            operation_expected_revision: open.lifecycle_revision,
            operation_request_digest,
            goal: DrainGoal::Sealed,
            initiating_actor: "operator:test".to_string(),
            initiated_at: 1,
            expected_binding: binding.clone(),
            expected_current_head_witness: open.current_head_witness.clone(),
            operation_request_payload,
            target_epoch_floor_by_shard: BTreeMap::from([(
                key.shard_id.to_string(),
                initial_epoch,
            )]),
            guarded_operation: None,
            seal_override: None,
        });
        draining.validate().unwrap();

        let prior_tail = draining.authenticated_wal_tail.clone();
        let prior_position = if sentinel_only {
            sentinel_position - 1
        } else {
            0
        };
        let entry_count = sentinel_position - prior_position;
        let segment_projection_digest = test_digest(if sentinel_only { '7' } else { '6' });
        let full_projection_digest = test_digest('7');
        let segment_digest = test_digest('3');
        let empty_fence_digest = test_digest('4');
        let tail_chain_digest = authenticated_wal_tail_chain_digest(
            &binding_scope_id,
            &key.enrollment_id.to_string(),
            &key.shard_id.to_string(),
            &stream_incarnation_id,
            &binding.stream_config_hash,
            &stream_physical_binding_digest(&binding).unwrap(),
            prior_position,
            sentinel_position,
            entry_count,
            &segment_digest,
            &prior_tail.chain_digest,
            1,
            &empty_fence_digest,
            &full_projection_digest,
        )
        .unwrap();
        let claim = ClaimReceipt::new(
            &draining.claim_receipt_chain,
            ClaimReceiptPreimage {
                graph_identity_digest,
                identity: key.identity,
                claim_id: ShardId::new_v4().to_string(),
                lifecycle_operation_id: Some(drain_id),
                binding_scope_id: binding_scope_id.clone(),
                enrollment_id: key.enrollment_id.to_string(),
                shard_id: key.shard_id.to_string(),
                stream_incarnation_id,
                stream_configuration_digest: binding.stream_config_hash.clone(),
                physical_binding_digest: stream_physical_binding_digest(&binding).unwrap(),
                recovery_operation_id: "passive-worker-test-claim".to_string(),
                claim_kind: "DRAIN_FENCE".to_string(),
                profile: ClaimProfile::RetainAll,
                claim_operation_digest: test_digest('2'),
                attempt_count: 1,
                attempt_chain_head_id: test_digest('8'),
                attempt_effect_chain_digest: test_digest('9'),
                terminal_attempt_id: ShardId::new_v4().to_string(),
                terminal_pre_shard_manifest_version: achieved_shard_manifest_version - 1,
                achieved_shard_manifest_version,
                achieved_writer_epoch,
                sentinel_position,
                sentinel_digest: test_digest('a'),
                replay_cursor: if sentinel_only {
                    sentinel_position - 1
                } else {
                    sentinel_position
                },
                authenticated_tail_prior_position: prior_position,
                authenticated_tail_position: sentinel_position,
                authenticated_tail_published_prefix_position: 0,
                authenticated_tail_segment_entry_count: entry_count,
                authenticated_tail_segment_digest: segment_digest,
                authenticated_tail_segment_lww_projection_digest: segment_projection_digest,
                authenticated_tail_prior_chain_digest: prior_tail.chain_digest,
                authenticated_tail_segment_count: 1,
                authenticated_tail_chain_digest: tail_chain_digest.clone(),
                authenticated_tail_empty_fence_state_digest: empty_fence_digest,
                authenticated_tail_lww_projection_digest: full_projection_digest.clone(),
                terminal_effect_digest: test_digest('b'),
                terminal_classification: ClaimTerminalClassification::StockManifestPlusSentinel,
                recorded_at: 2,
            },
        )
        .unwrap();
        let mut adopted = draining;
        adopted.lifecycle_revision += 1;
        adopted.claim_receipt_chain = claim.next_chain_ref().unwrap();
        adopted.current_claim_receipt_id = Some(claim.record_id.clone());
        adopted.authenticated_wal_tail = AuthenticatedWalTail {
            binding_scope_id,
            position: sentinel_position,
            segment_count: 1,
            chain_digest: tail_chain_digest,
            lww_projection_digest: full_projection_digest,
        };
        *adopted
            .epoch_floor_by_shard
            .get_mut(&key.shard_id.to_string())
            .unwrap() = achieved_writer_epoch;
        *adopted
            .drain
            .as_mut()
            .unwrap()
            .target_epoch_floor_by_shard
            .get_mut(&key.shard_id.to_string())
            .unwrap() = achieved_writer_epoch;
        adopted.validate().unwrap();
        (adopted, claim)
    }

    fn details(enrollment_id: ShardId) -> MemWalIndexDetails {
        MemWalIndexDetails {
            num_shards: 1,
            sharding_specs: vec![ShardingSpec {
                spec_id: UNSHARDED_SPEC_ID,
                fields: vec![ShardingField {
                    field_id: "bucket".to_string(),
                    source_ids: Vec::new(),
                    transform: Some("unsharded".to_string()),
                    expression: None,
                    result_type: "int32".to_string(),
                    parameters: HashMap::new(),
                }],
            }],
            writer_config_defaults: expected_b1_writer_defaults(enrollment_id)
                .into_iter()
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn passive_binding_inventory_accepts_only_selected_exact_commitment() {
        let historical = ShardId::new_v4();
        let current = ShardId::new_v4();
        let mut authenticated = vec![historical, current];
        authenticated.sort_unstable();
        let commitment = retained_shard_inventory_commitment(&authenticated).unwrap();

        require_committed_passive_shard_inventory(
            2,
            current,
            None,
            &[current],
            "initial inventory",
        )
        .unwrap();
        require_committed_passive_shard_inventory(
            3,
            current,
            Some(&commitment),
            &authenticated,
            "rebound inventory",
        )
        .unwrap();

        let mut reversed = authenticated.clone();
        reversed.reverse();
        assert!(
            require_committed_passive_shard_inventory(
                3,
                current,
                Some(&commitment),
                &reversed,
                "reversed inventory",
            )
            .is_err()
        );
        let mut with_foreign = authenticated.clone();
        with_foreign.push(ShardId::new_v4());
        with_foreign.sort_unstable();
        assert!(
            require_committed_passive_shard_inventory(
                3,
                current,
                Some(&commitment),
                &with_foreign,
                "foreign inventory",
            )
            .is_err()
        );
        assert!(
            require_committed_passive_shard_inventory(
                3,
                current,
                Some(&commitment),
                &authenticated[..1],
                "missing inventory",
            )
            .is_err()
        );
        let mut wrong_digest = commitment.clone();
        wrong_digest.retained_shard_set_digest = format!("sha256:{}", "0".repeat(64));
        assert!(
            require_committed_passive_shard_inventory(
                3,
                current,
                Some(&wrong_digest),
                &authenticated,
                "wrong digest",
            )
            .is_err()
        );
        assert!(
            require_committed_passive_shard_inventory(
                2,
                current,
                Some(&commitment),
                &[current],
                "initial with commitment",
            )
            .is_err()
        );
        assert!(
            require_committed_passive_shard_inventory(
                3,
                current,
                None,
                &authenticated,
                "rebind without commitment",
            )
            .is_err()
        );
    }

    #[test]
    fn passive_current_binding_uses_only_fixed_size_selected_commitment() {
        let historical = ShardId::new_v4();
        let current = ShardId::new_v4();
        let commitment = retained_shard_inventory_commitment(&[historical, current]).unwrap();

        require_selected_passive_shard_commitment(2, None).unwrap();
        require_selected_passive_shard_commitment(3, Some(&commitment)).unwrap();

        assert!(require_selected_passive_shard_commitment(3, None).is_err());
        assert!(require_selected_passive_shard_commitment(2, Some(&commitment)).is_err());

        let mut wrong_count = commitment;
        wrong_count.retained_shard_count += 1;
        assert!(require_selected_passive_shard_commitment(3, Some(&wrong_count)).is_err());
    }

    fn batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from_iter_values(
                    (0..rows).map(|row| format!("id-{row}")),
                )),
                Arc::new(Int32Array::from_iter_values(
                    (0..rows).map(|row| row as i32),
                )),
            ],
        )
        .unwrap()
    }

    fn limits() -> B1WorkerLimits {
        B1WorkerLimits {
            max_resident_writers_root: 4,
            max_resident_writers_per_table: 1,
            max_reserved_arrow_bytes: 4 * B1_MAX_GENERATION_ARROW_BYTES,
            max_b2_preprocessing_bytes: B2_MAX_PREPROCESSING_BYTES_ROOT,
            max_inflight_calls: 2,
            max_pending_generations: 4,
            put_deadline: Duration::from_secs(1),
            seal_deadline: Duration::from_secs(2),
            abort_deadline: Duration::from_secs(1),
            idle_timeout: Duration::from_secs(30),
        }
    }

    struct EnrolledWorkerFixture {
        _dir: tempfile::TempDir,
        uri: String,
        dataset: Dataset,
        details: MemWalIndexDetails,
        plan: MemWalEnrollmentPlan,
        key: StreamWorkerKey,
        initial_epoch: u64,
    }

    async fn fresh_worker_dataset(uri: &str) -> Dataset {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false).with_metadata(HashMap::from([(
                LANCE_UNENFORCED_PRIMARY_KEY.to_string(),
                "true".to_string(),
            )])),
            Field::new("value", DataType::Int32, false),
        ]));
        let seed = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["seed"])),
                Arc::new(Int32Array::from(vec![0])),
            ],
        )
        .unwrap();
        let reader = RecordBatchIterator::new([Ok(seed)], schema);
        // forbidden-api-allow: test-only exact worker fixture construction
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

    async fn enrolled_worker_fixture(label: &str) -> EnrolledWorkerFixture {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(format!("{label}.lance"));
        let uri = path.to_str().unwrap().to_string();
        let mut dataset = fresh_worker_dataset(&uri).await;
        let before = capture_pre_enrollment_head(&dataset).await.unwrap();
        let plan = MemWalEnrollmentPlan::new(ShardId::new_v4(), ShardId::new_v4()).unwrap();
        initialize_index_from_exact_no_effect(&mut dataset, &before, &plan)
            .await
            .unwrap();
        let (_, shard) = provision_shard_from_exact_index_only(&dataset, &before, &plan)
            .await
            .unwrap();
        // forbidden-api-allow: test-only raw Lance fixture reopen
        let dataset = Dataset::open(&uri).await.unwrap();
        let details = dataset.mem_wal_index_details().await.unwrap().unwrap();
        let key = StreamWorkerKey::new(
            TableIdentity::new(59, 61).unwrap(),
            plan.enrollment_id,
            plan.shard_id,
        )
        .unwrap();
        EnrolledWorkerFixture {
            _dir: dir,
            uri,
            dataset,
            details,
            plan,
            key,
            initial_epoch: shard.writer_epoch,
        }
    }

    fn fresh_worker_opener(
        dataset: Dataset,
        table_root_uri: String,
        details: MemWalIndexDetails,
        enrollment_id: ShardId,
        shard_id: ShardId,
        epoch_floor: u64,
    ) -> WorkerOpener {
        Box::new(move || {
            Box::pin(async move {
                let config = reconstruct_b1_writer_config(&details, enrollment_id, shard_id)
                    .map_err(WorkerOpenFailure::unclaimed)?;
                let writer = dataset
                    .mem_wal_writer(shard_id, config)
                    .await
                    .map_err(|error| {
                        WorkerOpenFailure::unclaimed(MemWalWorkerError::lance(
                            "test writer claim",
                            error,
                        ))
                    })?;
                OpenedMemWalWorker::classify(
                    writer,
                    &dataset,
                    &table_root_uri,
                    &details,
                    epoch_floor,
                )
                .await
            })
        })
    }

    async fn claim_opened_worker(
        dataset: &Dataset,
        table_root_uri: &str,
        details: &MemWalIndexDetails,
        plan: &MemWalEnrollmentPlan,
        epoch_floor: u64,
    ) -> OpenedMemWalWorker {
        let config =
            reconstruct_b1_writer_config(details, plan.enrollment_id, plan.shard_id).unwrap();
        let writer = dataset.mem_wal_writer(plan.shard_id, config).await.unwrap();
        match OpenedMemWalWorker::classify(writer, dataset, table_root_uri, details, epoch_floor)
            .await
        {
            Ok(opened) => opened,
            Err(failure) => {
                let (error, _) = failure.into_parts();
                panic!("test writer classification failed: {error}");
            }
        }
    }

    async fn install_cached_worker(
        registry: &Arc<MemWalWorkerRegistry>,
        key: StreamWorkerKey,
        opened: OpenedMemWalWorker,
    ) -> Arc<MemWalWorker> {
        assert!(matches!(
            &opened.disposition,
            ActiveStreamDisposition::AdmitEmpty
        ));
        registry.reserve_resident(key.identity).unwrap();
        let worker = Arc::new(MemWalWorker::from_opened(
            key,
            "node:Test".to_string(),
            opened,
        ));
        registry.register_initial_worker_usage(&worker).unwrap();
        let slot = registry.slot(key);
        let mut state = slot.state.lock().await;
        assert!(matches!(&*state, RegistrySlotState::Vacant));
        *state = RegistrySlotState::Active(Arc::clone(&worker));
        worker
    }

    async fn assert_real_empty_quiesce(
        fixture: &EnrolledWorkerFixture,
        expected_generation: u64,
        expected_cursor: u64,
    ) -> u64 {
        let registry = MemWalWorkerRegistry::for_root(
            &format!(
                "memory://real-empty-quiesce/{}/{}",
                fixture.key,
                ShardId::new_v4()
            ),
            limits(),
        )
        .unwrap();
        let opened = claim_opened_worker(
            &fixture.dataset,
            &fixture.uri,
            &fixture.details,
            &fixture.plan,
            fixture.initial_epoch,
        )
        .await;
        let old_worker = install_cached_worker(&registry, fixture.key, opened).await;
        let old_epoch = old_worker.writer.epoch();
        let old_manifest = old_worker.writer.manifest().await.unwrap().unwrap();
        assert_eq!(old_manifest.current_generation, expected_generation);
        assert_eq!(
            old_manifest.replay_after_wal_entry_position,
            expected_cursor
        );

        let admission = Arc::new(tokio::sync::RwLock::new(()));
        let exclusive = Arc::clone(&admission).write_owned().await;
        let opener = fresh_worker_opener(
            fixture.dataset.clone(),
            fixture.uri.clone(),
            fixture.details.clone(),
            fixture.plan.enrollment_id,
            fixture.plan.shard_id,
            fixture.initial_epoch,
        );
        let cut = registry
            .quiesce_cut(
                fixture.key,
                "node:Test".to_string(),
                CheckedExclusiveStreamAuthority::from_exclusive_admission(exclusive),
                opener,
            )
            .await
            .unwrap();
        let QuiesceCut::Empty(cut) = cut else {
            panic!("an empty reopened shard must return EmptyFenceCut");
        };
        assert_eq!(cut.key, fixture.key);
        assert!(
            cut.writer_epoch > old_epoch,
            "quiesce must retire epoch {old_epoch} and cut from a fresh successor, got {}",
            cut.writer_epoch
        );
        assert!(
            cut.shard_manifest_version > old_manifest.version,
            "the fresh epoch claim must advance the shard manifest"
        );
        assert_eq!(cut.current_generation, expected_generation);
        assert_eq!(cut.replay_after_wal_entry_position, expected_cursor);
        assert!(
            old_worker.writer.check_fenced().await.is_err(),
            "the cached prior owner must be fenced by the fresh claim"
        );
        {
            let slot = registry.slot(fixture.key);
            assert!(matches!(
                &*slot.state.lock().await,
                RegistrySlotState::Vacant
            ));
            let usage = registry.usage.lock().unwrap();
            assert_eq!(usage.resident_writers_root, 0);
            assert_eq!(usage.reserved_arrow_bytes, 0);
            assert_eq!(usage.pending_generations, 0);
            assert_eq!(usage.inflight_calls, 0);
        }
        assert!(
            tokio::time::timeout(
                Duration::from_millis(20),
                Arc::clone(&admission).read_owned(),
            )
            .await
            .is_err(),
            "the returned empty cut must retain exclusive admission"
        );
        let fresh_epoch = cut.writer_epoch;
        let authority = cut.into_exclusive_authority();
        assert!(
            tokio::time::timeout(
                Duration::from_millis(20),
                Arc::clone(&admission).read_owned(),
            )
            .await
            .is_err(),
            "extracting authority must transfer rather than release it"
        );
        drop(authority);
        tokio::time::timeout(Duration::from_secs(2), admission.read_owned())
            .await
            .expect("dropping extracted authority must release admission");
        fresh_epoch
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resumed_empty_writer_uses_the_normal_idle_eviction_path() {
        let fixture = enrolled_worker_fixture("resumed-idle-eviction").await;
        let mut bounded = limits();
        bounded.max_resident_writers_root = 1;
        bounded.max_resident_writers_per_table = 1;
        bounded.idle_timeout = Duration::from_millis(20);
        let registry = MemWalWorkerRegistry::for_root(
            &format!("memory://resumed-idle-eviction/{}", ShardId::new_v4()),
            bounded,
        )
        .unwrap();
        let admission = Arc::new(tokio::sync::RwLock::new(()));
        let exclusive = Arc::clone(&admission).write_owned().await;
        let opener = fresh_worker_opener(
            fixture.dataset.clone(),
            fixture.uri.clone(),
            fixture.details.clone(),
            fixture.plan.enrollment_id,
            fixture.plan.shard_id,
            fixture.initial_epoch,
        );
        let idle_admission = Arc::clone(&admission);
        let idle_authority: IdleAuthorityCheck = Arc::new(move |_writer| {
            let admission = Arc::clone(&idle_admission);
            Box::pin(async move {
                let shared = admission.read_owned().await;
                Ok(CheckedStreamAuthority::from_shared_admission(shared))
            })
        });

        registry
            .install_resumed_writer(
                fixture.key,
                "node:Test".to_string(),
                CheckedExclusiveStreamAuthority::from_exclusive_admission(exclusive),
                opener,
                idle_authority,
            )
            .await
            .unwrap();
        assert_eq!(registry.usage.lock().unwrap().resident_writers_root, 1);

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let released = registry.usage.lock().unwrap().resident_writers_root == 0;
                let slot = registry.slot(fixture.key);
                let vacant = matches!(&*slot.state.lock().await, RegistrySlotState::Vacant);
                if released && vacant {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("an unused resumed writer must release the sole resident slot");

        let other_identity = TableIdentity::new(63, 65).unwrap();
        registry.reserve_resident(other_identity).unwrap();
        registry.release_resident_reservation(other_identity);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn quiesce_releases_idle_owner_before_the_idle_deadline() {
        let fixture = enrolled_worker_fixture("quiesce-wakes-idle-owner").await;
        let mut bounded = limits();
        bounded.idle_timeout = Duration::from_secs(30);
        let registry = MemWalWorkerRegistry::for_root(
            &format!("memory://quiesce-wakes-idle-owner/{}", ShardId::new_v4()),
            bounded,
        )
        .unwrap();
        let opened = claim_opened_worker(
            &fixture.dataset,
            &fixture.uri,
            &fixture.details,
            &fixture.plan,
            fixture.initial_epoch,
        )
        .await;
        let worker = install_cached_worker(&registry, fixture.key, opened).await;
        let slot = registry.slot(fixture.key);

        // This token models the checked graph runtime captured by the real
        // idle-authority factory. Its only strong owner after arming is the
        // spawned idle task.
        let retained_runtime = Arc::new(());
        let runtime_probe = Arc::downgrade(&retained_runtime);
        let idle_authority: IdleAuthorityCheck = Arc::new(move |_writer| {
            let retained_runtime = Arc::clone(&retained_runtime);
            Box::pin(async move {
                let _retained_runtime = retained_runtime;
                std::future::pending::<Result<CheckedStreamAuthority, IdleAuthorityFailure>>().await
            })
        });
        registry.ensure_idle_task(&slot, &worker, idle_authority);
        tokio::task::yield_now().await;
        assert!(
            runtime_probe.upgrade().is_some(),
            "the armed idle owner must retain its authority factory"
        );

        let admission = Arc::new(tokio::sync::RwLock::new(()));
        let exclusive = Arc::clone(&admission).write_owned().await;
        let opener = fresh_worker_opener(
            fixture.dataset.clone(),
            fixture.uri.clone(),
            fixture.details.clone(),
            fixture.plan.enrollment_id,
            fixture.plan.shard_id,
            fixture.initial_epoch,
        );
        let cut = registry
            .quiesce_cut(
                fixture.key,
                "node:Test".to_string(),
                CheckedExclusiveStreamAuthority::from_exclusive_admission(exclusive),
                opener,
            )
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_millis(500), async {
            while runtime_probe.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("quiesce must wake and release the idle owner before its 30-second deadline");
        drop(cut);
    }

    async fn assert_ordinary_seal_still_refuses_empty(fixture: &EnrolledWorkerFixture) {
        let registry = MemWalWorkerRegistry::for_root(
            &format!(
                "memory://ordinary-empty-seal/{}/{}",
                fixture.key,
                ShardId::new_v4()
            ),
            limits(),
        )
        .unwrap();
        let admission = Arc::new(tokio::sync::RwLock::new(()));
        let exclusive = Arc::clone(&admission).write_owned().await;
        let opener = fresh_worker_opener(
            fixture.dataset.clone(),
            fixture.uri.clone(),
            fixture.details.clone(),
            fixture.plan.enrollment_id,
            fixture.plan.shard_id,
            fixture.initial_epoch,
        );
        let error = match registry
            .seal_and_drain(
                fixture.key,
                "node:Test".to_string(),
                CheckedExclusiveStreamAuthority::from_exclusive_admission(exclusive),
                opener,
            )
            .await
        {
            Ok(_) => panic!("ordinary seal_and_drain must continue to refuse an empty generation"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("cannot seal an empty admitted generation")
        );
        tokio::time::timeout(Duration::from_secs(2), admission.read_owned())
            .await
            .expect("empty refusal must retire its claimed writer and release admission");
        let slot = registry.slot(fixture.key);
        assert!(matches!(
            &*slot.state.lock().await,
            RegistrySlotState::Vacant
        ));
    }

    async fn fold_and_reopen_empty_fixture(fixture: &mut EnrolledWorkerFixture) -> u64 {
        let config = reconstruct_b1_writer_config(
            &fixture.details,
            fixture.plan.enrollment_id,
            fixture.plan.shard_id,
        )
        .unwrap();
        let writer = fixture
            .dataset
            .mem_wal_writer(fixture.plan.shard_id, config)
            .await
            .unwrap();
        assert!(writer.epoch() > fixture.initial_epoch);
        let (_, watcher) = writer.put_no_wait(vec![batch(1)]).await.unwrap();
        watcher
            .expect("durable test writer must return a watcher")
            .wait()
            .await
            .unwrap();
        writer.check_fenced().await.unwrap();
        let expected_cursor = writer.wal_stats().next_wal_entry_position.saturating_sub(1);
        writer.force_seal_active().await.unwrap();
        writer.wait_for_flush_drain().await.unwrap();
        let physical_cut = prove_post_drain_cut(&writer, 1, Some(expected_cursor))
            .await
            .unwrap();
        assert_eq!(physical_cut.generation, 1);
        writer.abort().await.unwrap();

        let source = batch(1);
        let reader = RecordBatchIterator::new([Ok(source.clone())], source.schema());
        let mut builder =
            // forbidden-api-allow: test-only raw Lance fixture mutation
            MergeInsertBuilder::try_new(Arc::new(fixture.dataset.clone()), vec!["id".to_string()])
                .unwrap();
        builder
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .use_index(false)
            .conflict_retries(0)
            .mark_generations_as_merged(vec![MergedGeneration::new(
                fixture.plan.shard_id,
                physical_cut.generation,
            )]);
        let (dataset, stats) = builder
            .try_build()
            .unwrap()
            .execute_reader(Box::new(reader))
            .await
            .unwrap();
        assert_eq!(stats.num_inserted_rows, 1);
        drop(dataset);

        // forbidden-api-allow: test-only raw Lance fixture reopen
        fixture.dataset = Dataset::open(&fixture.uri).await.unwrap();
        fixture.details = fixture
            .dataset
            .mem_wal_index_details()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            fixture.details.merged_generations,
            vec![MergedGeneration::new(fixture.plan.shard_id, 1)]
        );
        expected_cursor
    }

    #[test]
    fn config_v2_reconstruction_is_exact_and_no_roll() {
        let enrollment_id = ShardId::new_v4();
        let shard_id = ShardId::new_v4();
        let persisted = details(enrollment_id);
        let config = reconstruct_b1_writer_config(&persisted, enrollment_id, shard_id).unwrap();
        assert_eq!(config.shard_id, shard_id);
        assert_eq!(config.shard_spec_id, UNSHARDED_SPEC_ID);
        assert!(config.durable_write);
        assert!(config.sync_indexed_write);
        assert_eq!(config.max_wal_buffer_size, 10 * 1024 * 1024);
        assert_eq!(
            config.max_wal_flush_interval,
            Some(Duration::from_millis(100))
        );
        assert_eq!(config.max_memtable_size, 1024 * 1024 * 1024);
        assert_eq!(config.max_memtable_rows, 8_193);
        assert_eq!(config.max_memtable_batches, 8_193);
        assert_eq!(config.max_unflushed_memtable_bytes, 1024 * 1024 * 1024);
        assert!(config.enable_memtable);
        assert!(config.hnsw_params.is_empty());
        assert!(config.warmer.is_none());
        assert!(config.store_params.is_none());
        assert!(config.session.is_none());

        let mut foreign = persisted;
        foreign
            .writer_config_defaults
            .insert("foreign".to_string(), "default".to_string());
        assert!(matches!(
            reconstruct_b1_writer_config(&foreign, enrollment_id, shard_id),
            Err(MemWalWorkerError::InvalidConfig { .. })
        ));

        let mut foreign_progress = details(enrollment_id);
        foreign_progress.merged_generations = vec![MergedGeneration::new(ShardId::new_v4(), 1)];
        assert!(matches!(
            validate_bound_merge_progress(&foreign_progress, shard_id),
            Err(MemWalWorkerError::InvalidState { .. })
        ));
        assert!(matches!(
            reconstruct_b1_writer_config(&foreign_progress, enrollment_id, shard_id),
            Err(MemWalWorkerError::InvalidState { .. })
        ));

        let mut bound_progress = details(enrollment_id);
        bound_progress.merged_generations = vec![MergedGeneration::new(shard_id, 1)];
        reconstruct_b1_writer_config(&bound_progress, enrollment_id, shard_id).unwrap();
    }

    #[test]
    fn accounting_charges_exact_post_tombstone_logical_slice_memory() {
        let batch = batch(3);
        let charged = post_tombstone_accounting(&batch).unwrap();
        let schema = schema_with_tombstone(batch.schema().as_ref());
        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(BooleanArray::from(vec![false; 3])));
        let stored = RecordBatch::try_new(schema, columns).unwrap();
        assert_eq!(charged.rows, 3);
        assert_eq!(charged.batches, 1);
        assert_eq!(
            charged.arrow_bytes,
            b1_logical_batch_bytes(&stored).unwrap()
        );
    }

    #[test]
    fn ordinal_range_is_exact_and_non_empty() {
        assert_eq!(CallerOrdinalRange::new(9, 11).unwrap().len().unwrap(), 3);
        assert!(CallerOrdinalRange::new(11, 9).is_err());
    }

    #[test]
    fn root_registry_is_shared_weakly() {
        let root = format!("memory://worker-registry/{}", ShardId::new_v4());
        let first = MemWalWorkerRegistry::for_root(&root, limits()).unwrap();
        let second = MemWalWorkerRegistry::for_root(&root, limits()).unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        let other = MemWalWorkerRegistry::for_root(&format!("{root}/other"), limits()).unwrap();
        assert!(!Arc::ptr_eq(&first, &other));

        let mut mismatched = limits();
        mismatched.max_inflight_calls += 1;
        assert!(matches!(
            MemWalWorkerRegistry::for_root(&root, mismatched),
            Err(MemWalWorkerError::InvalidConfig { .. })
        ));
    }

    #[test]
    fn worker_limits_are_required_and_counters_reject_before_effect() {
        let mut invalid = limits();
        invalid.put_deadline = Duration::ZERO;
        assert!(matches!(
            invalid.validate(),
            Err(MemWalWorkerError::InvalidConfig { .. })
        ));

        let root = format!("memory://worker-limits/{}", ShardId::new_v4());
        let mut one_inflight = limits();
        one_inflight.max_inflight_calls = 1;
        let registry = MemWalWorkerRegistry::for_root(&root, one_inflight).unwrap();
        let first = registry.reserve_inflight_core().unwrap();
        assert!(matches!(
            registry.reserve_inflight_core(),
            Err(MemWalWorkerError::ResourceLimit {
                resource: "stream_inflight_calls",
                ..
            })
        ));
        drop(first);
        assert!(registry.reserve_inflight_core().is_ok());

        let identity = TableIdentity::new(7, 9).unwrap();
        registry.reserve_resident(identity).unwrap();
        assert!(matches!(
            registry.reserve_resident(identity),
            Err(MemWalWorkerError::ResourceLimit {
                resource: "stream_resident_writers_per_table",
                ..
            })
        ));
        registry.release_resident_reservation(identity);
    }

    #[test]
    fn b2_preprocessing_is_root_bounded_before_payload_allocation() {
        let root = format!("memory://b2-preprocessing/{}", ShardId::new_v4());
        let mut preprocessing_limits = limits();
        preprocessing_limits.max_inflight_calls = 3;
        let registry = MemWalWorkerRegistry::for_root(&root, preprocessing_limits).unwrap();
        let first = registry.reserve_b2_preprocessing().unwrap();
        let second = registry.reserve_b2_preprocessing().unwrap();
        let error = match registry.reserve_b2_preprocessing() {
            Ok(_) => panic!("a third worst-case B2 envelope must exceed the root bound"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            OmniError::ResourceLimitExceeded {
                ref resource,
                limit,
                actual,
            } if resource == "stream_b2_preprocessing_bytes"
                && limit == B2_MAX_PREPROCESSING_BYTES_ROOT
                && actual == 3 * B2_PREPROCESSING_RESERVATION_BYTES
        ));
        {
            let usage = registry.usage.lock().unwrap();
            assert_eq!(
                usage.b2_preprocessing_bytes,
                B2_MAX_PREPROCESSING_BYTES_ROOT
            );
            // The failed third reservation releases the inflight slot it
            // acquired before discovering root scratch pressure.
            assert_eq!(usage.inflight_calls, 2);
        }
        drop(first);
        drop(second);
        let usage = registry.usage.lock().unwrap();
        assert_eq!(usage.b2_preprocessing_bytes, 0);
        assert_eq!(usage.inflight_calls, 0);
    }

    #[test]
    fn queued_input_is_charged_before_detach_and_released_without_transfer() {
        let root = format!("memory://queued-input/{}", ShardId::new_v4());
        let registry = MemWalWorkerRegistry::for_root(&root, limits()).unwrap();
        let key = StreamWorkerKey::new(
            TableIdentity::new(31, 37).unwrap(),
            ShardId::new_v4(),
            ShardId::new_v4(),
        )
        .unwrap();
        let input = batch(3);
        let expected = b1_input_accounting(&input).unwrap();
        let inflight = registry.reserve_inflight_core().unwrap();
        let permit = registry
            .reserve_queued_charge(key, "node:Test", expected, inflight, None)
            .unwrap();
        assert_eq!(permit.charge(), expected);
        assert_eq!(
            registry.usage.lock().unwrap().reserved_arrow_bytes,
            expected.arrow_bytes
        );
        drop(permit);
        assert_eq!(registry.usage.lock().unwrap().reserved_arrow_bytes, 0);

        let bounded_root = format!("memory://queued-input-cap/{}", ShardId::new_v4());
        let mut bounded_limits = limits();
        bounded_limits.max_reserved_arrow_bytes = B1_MAX_GENERATION_ARROW_BYTES;
        let bounded = MemWalWorkerRegistry::for_root(&bounded_root, bounded_limits).unwrap();
        let first = GenerationAccounting {
            rows: 1,
            arrow_bytes: 30 * 1024 * 1024,
            batches: 1,
        };
        let second = GenerationAccounting {
            rows: 1,
            arrow_bytes: 3 * 1024 * 1024,
            batches: 1,
        };
        let first_inflight = bounded.reserve_inflight_core().unwrap();
        let first_permit = bounded
            .reserve_queued_charge(key, "node:Test", first, first_inflight, None)
            .unwrap();
        let second_inflight = bounded.reserve_inflight_core().unwrap();
        let error =
            match bounded.reserve_queued_charge(key, "node:Test", second, second_inflight, None) {
                Ok(_) => panic!("same-key queued projection must refuse the byte-cap crossing"),
                Err(error) => error,
            };
        assert!(matches!(error, OmniError::FoldRequired { .. }), "{error:?}");
        let other_key = StreamWorkerKey::new(
            TableIdentity::new(41, 43).unwrap(),
            ShardId::new_v4(),
            ShardId::new_v4(),
        )
        .unwrap();
        let cross_key_inflight = bounded.reserve_inflight_core().unwrap();
        let error = match bounded.reserve_queued_charge(
            other_key,
            "node:Other",
            second,
            cross_key_inflight,
            None,
        ) {
            Ok(_) => panic!("cross-key pressure must retain the aggregate resource error"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            OmniError::ResourceLimitExceeded { ref resource, .. }
                if resource == "stream_reserved_arrow_bytes"
        ));
        drop(first_permit);
        assert!(bounded.usage.lock().unwrap().reserved_by_key.is_empty());

        let oversized = batch(B1_MAX_GENERATION_ROWS as usize + 1);
        let oversized = b1_input_accounting(&oversized).unwrap();
        assert_eq!(oversized.rows, B1_MAX_GENERATION_ROWS + 1);
        assert!(!oversized.fits());
        assert_eq!(registry.usage.lock().unwrap().reserved_arrow_bytes, 0);
    }

    #[test]
    fn fold_reservation_promotes_to_full_cap_before_effect() {
        let identity = TableIdentity::new(17, 19).unwrap();
        let key = StreamWorkerKey::new(identity, ShardId::new_v4(), ShardId::new_v4()).unwrap();
        let initial = GenerationAccounting {
            rows: 1,
            arrow_bytes: 1_024,
            batches: 1,
        };
        let mut registry = RegistryUsage {
            resident_writers_root: 1,
            resident_writers_by_table: HashMap::from([(identity, 1)]),
            reserved_arrow_bytes: 1_024,
            b2_preprocessing_bytes: 0,
            reserved_by_key: HashMap::from([(key, initial)]),
            fold_required_by_key: HashMap::new(),
            inflight_calls: 1,
            pending_generations: 1,
        };
        let mut worker = WorkerUsage {
            resident: true,
            accounting: initial,
            pending_generations: 1,
            generation_registered: true,
        };
        promote_fold_usage_counters(&mut registry, &mut worker, key, &limits()).unwrap();
        assert_eq!(worker.accounting.arrow_bytes, B1_MAX_GENERATION_ARROW_BYTES);
        assert_eq!(registry.reserved_arrow_bytes, B1_MAX_GENERATION_ARROW_BYTES);
        // Promotion is idempotent for a retry before physical effect.
        promote_fold_usage_counters(&mut registry, &mut worker, key, &limits()).unwrap();
        assert_eq!(registry.reserved_arrow_bytes, B1_MAX_GENERATION_ARROW_BYTES);

        let mut constrained = limits();
        constrained.max_reserved_arrow_bytes = B1_MAX_GENERATION_ARROW_BYTES;
        let other_key = StreamWorkerKey::new(
            TableIdentity::new(18, 20).unwrap(),
            ShardId::new_v4(),
            ShardId::new_v4(),
        )
        .unwrap();
        let second_key = StreamWorkerKey::new(
            TableIdentity::new(21, 22).unwrap(),
            ShardId::new_v4(),
            ShardId::new_v4(),
        )
        .unwrap();
        let second_accounting = GenerationAccounting {
            rows: 1,
            arrow_bytes: 1,
            batches: 1,
        };
        let mut aggregate = RegistryUsage {
            reserved_arrow_bytes: B1_MAX_GENERATION_ARROW_BYTES,
            reserved_by_key: HashMap::from([
                (
                    other_key,
                    GenerationAccounting {
                        rows: 1,
                        arrow_bytes: B1_MAX_GENERATION_ARROW_BYTES - 1,
                        batches: 1,
                    },
                ),
                (second_key, second_accounting),
            ]),
            pending_generations: 2,
            ..RegistryUsage::default()
        };
        let mut second = WorkerUsage {
            resident: true,
            accounting: second_accounting,
            pending_generations: 1,
            generation_registered: true,
        };
        assert!(matches!(
            promote_fold_usage_counters(&mut aggregate, &mut second, second_key, &constrained),
            Err(MemWalWorkerError::ResourceLimit {
                resource: "stream_reserved_arrow_bytes",
                ..
            })
        ));
        assert_eq!(second.accounting.arrow_bytes, 1);
    }

    #[test]
    fn fold_usage_permit_holds_resident_pending_bytes_and_inflight_until_drop() {
        let root = format!("memory://fold-usage/{}", ShardId::new_v4());
        let registry = MemWalWorkerRegistry::for_root(&root, limits()).unwrap();
        let identity = TableIdentity::new(23, 29).unwrap();
        let key = StreamWorkerKey::new(identity, ShardId::new_v4(), ShardId::new_v4()).unwrap();
        let accounting = GenerationAccounting {
            rows: 1,
            arrow_bytes: B1_MAX_GENERATION_ARROW_BYTES,
            batches: 1,
        };
        registry.reserve_resident(identity).unwrap();
        {
            let mut usage = registry.usage.lock().unwrap();
            usage.reserved_arrow_bytes = B1_MAX_GENERATION_ARROW_BYTES;
            usage.reserved_by_key.insert(key, accounting);
            usage.pending_generations = 1;
        }
        let inflight = registry.reserve_inflight_core().unwrap();
        let permit = FoldUsagePermit {
            registry: Arc::clone(&registry),
            key,
            usage: WorkerUsage {
                resident: true,
                accounting,
                pending_generations: 1,
                generation_registered: true,
            },
            _inflight: inflight,
        };
        {
            let usage = registry.usage.lock().unwrap();
            assert_eq!(usage.resident_writers_root, 1);
            assert_eq!(usage.reserved_arrow_bytes, B1_MAX_GENERATION_ARROW_BYTES);
            assert_eq!(usage.pending_generations, 1);
            assert_eq!(usage.inflight_calls, 1);
        }
        drop(permit);
        let usage = registry.usage.lock().unwrap();
        assert_eq!(usage.resident_writers_root, 0);
        assert_eq!(usage.reserved_arrow_bytes, 0);
        assert_eq!(usage.pending_generations, 0);
        assert_eq!(usage.inflight_calls, 0);
        assert!(usage.reserved_by_key.is_empty());
        assert!(!usage.resident_writers_by_table.contains_key(&identity));
    }

    #[tokio::test]
    async fn deadline_continuation_owns_token_until_one_settled_handoff() {
        struct DropToken(Arc<AtomicUsize>);
        impl Drop for DropToken {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::AcqRel);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let settle = Arc::new(tokio::sync::Notify::new());
        let (continued, observed) = tokio::sync::oneshot::channel();
        let settle_in_task = Arc::clone(&settle);
        let drops_in_task = Arc::clone(&drops);
        spawn_after_settle(
            DropToken(Arc::clone(&drops)),
            async move { settle_in_task.notified().await },
            move |token| async move {
                assert_eq!(drops_in_task.load(Ordering::Acquire), 0);
                let _ = continued.send(());
                drop(token);
            },
        );

        tokio::task::yield_now().await;
        assert_eq!(drops.load(Ordering::Acquire), 0);
        settle.notify_one();
        tokio::time::timeout(Duration::from_secs(1), observed)
            .await
            .expect("continuation must run")
            .expect("continuation signal");
        assert_eq!(drops.load(Ordering::Acquire), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn quiesce_never_written_enrolled_shard_uses_fresh_empty_epoch() {
        let fixture = enrolled_worker_fixture("never-written-empty-quiesce").await;
        assert_eq!(fixture.initial_epoch, 1);
        let fresh_epoch = assert_real_empty_quiesce(&fixture, 1, 0).await;
        assert!(
            fresh_epoch >= 3,
            "one cached owner must be retired before the fresh empty cut"
        );
        assert_ordinary_seal_still_refuses_empty(&fixture).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn quiesce_folded_reopened_empty_shard_uses_fresh_empty_epoch() {
        let mut fixture = enrolled_worker_fixture("folded-reopened-empty-quiesce").await;
        let cursor = fold_and_reopen_empty_fixture(&mut fixture).await;
        assert!(cursor > 0);
        let fresh_epoch = assert_real_empty_quiesce(&fixture, 2, cursor).await;
        assert!(
            fresh_epoch >= 4,
            "fold writer and cached owner must both precede the fresh empty cut"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reopened_flushed_data_generation_replays_full_current_projection() {
        let mut fixture = enrolled_worker_fixture("flushed-projection-reopen").await;
        let config = reconstruct_b1_writer_config(
            &fixture.details,
            fixture.plan.enrollment_id,
            fixture.plan.shard_id,
        )
        .unwrap();
        let writer = fixture
            .dataset
            .mem_wal_writer(fixture.plan.shard_id, config)
            .await
            .unwrap();
        let flushed_epoch = writer.epoch();
        let (_, watcher) = writer.put_no_wait(vec![batch(1)]).await.unwrap();
        watcher
            .expect("durable test writer must return a watcher")
            .wait()
            .await
            .unwrap();
        writer.check_fenced().await.unwrap();
        let expected_cursor = writer.wal_stats().next_wal_entry_position.saturating_sub(1);
        writer.force_seal_active().await.unwrap();
        writer.wait_for_flush_drain().await.unwrap();
        let cut = prove_post_drain_cut(&writer, 1, Some(expected_cursor))
            .await
            .unwrap();
        assert_eq!(cut.generation, 1);
        writer.abort().await.unwrap();

        // forbidden-api-allow: test-only raw Lance fixture reopen
        fixture.dataset = Dataset::open(&fixture.uri).await.unwrap();
        fixture.details = fixture
            .dataset
            .mem_wal_index_details()
            .await
            .unwrap()
            .unwrap();
        let config = reconstruct_b1_writer_config(
            &fixture.details,
            fixture.plan.enrollment_id,
            fixture.plan.shard_id,
        )
        .unwrap();
        let successor = fixture
            .dataset
            .mem_wal_writer(fixture.plan.shard_id, config)
            .await
            .unwrap();
        let mut opened = match OpenedMemWalWorker::classify(
            successor,
            &fixture.dataset,
            &fixture.uri,
            &fixture.details,
            flushed_epoch,
        )
        .await
        {
            Ok(opened) => opened,
            Err(failure) => {
                let (error, _) = failure.into_parts();
                panic!("flushed successor classification failed: {error}");
            }
        };
        assert!(matches!(
            &opened.disposition,
            ActiveStreamDisposition::FoldOnlyFlushed(flushed)
                if flushed.generation == cut.generation && flushed.path == cut.path
        ));
        let CurrentGenerationProjectionSource::Replay(batches) = opened
            .current_generation_projection_source()
            .expect("flushed projection must be available exactly once")
        else {
            panic!("a data-bearing flushed generation must replay its full projection");
        };
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let ids = batches[0]
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let tombstones = batches[0]
            .column_by_name(lance::dataset::mem_wal::TOMBSTONE)
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(ids.value(0), "id-0");
        assert!(!tombstones.value(0));
        assert!(
            opened.current_generation_projection_source().is_err(),
            "the generation-sized projection must not remain resident after claim publication takes it"
        );
        opened.writer().abort().await.unwrap();
    }

    #[tokio::test]
    async fn passive_quiesce_reuses_current_claim_flushed_generation_without_fresh_claim() {
        let key = StreamWorkerKey::new(
            TableIdentity::new(67, 71).unwrap(),
            ShardId::new_v4(),
            ShardId::new_v4(),
        )
        .unwrap();
        let (draining, claim) = passive_test_drain_and_claim(key, 7, 3, 2, false);
        validate_passive_drain_claim(key, &draining, &claim).unwrap();
        let registry = MemWalWorkerRegistry::for_root(
            &format!("memory://passive-flushed/{}", ShardId::new_v4()),
            limits(),
        )
        .unwrap();
        let admission = Arc::new(tokio::sync::RwLock::new(()));
        let exclusive = Arc::clone(&admission).write_owned().await;
        let inflight = registry.reserve_inflight_core().unwrap();
        let disposition = registry
            .passive_disposition_from_physical(
                key,
                CheckedExclusiveStreamAuthority::from_exclusive_admission(exclusive),
                &draining,
                &claim,
                PassiveB1PhysicalState::FoldOnlyFlushed(FlushedGenerationState {
                    shard_manifest_version: 8,
                    writer_epoch: 3,
                    generation: 1,
                    path: "_mem_wal/test/generation-1.lance".to_string(),
                    replay_after_wal_entry_position: 2,
                }),
                inflight,
            )
            .unwrap();
        let PassiveQuiesceDisposition::Reusable(QuiesceCut::Generation(cut)) = disposition else {
            panic!("the exact already-flushed claim cut must be reusable");
        };
        assert_eq!(cut.writer_epoch, 3);
        assert_eq!(cut.shard_manifest_version, 8);
        assert_eq!(cut.generation, 1);
        assert_eq!(cut.replay_after_wal_entry_position, 2);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(20),
                Arc::clone(&admission).read_owned()
            )
            .await
            .is_err(),
            "the reusable generation cut must retain exclusive admission"
        );
        drop(cut.into_exclusive_authority());
        tokio::time::timeout(Duration::from_secs(1), admission.read_owned())
            .await
            .expect("dropping transferred authority must release admission");
        let usage = registry.usage.lock().unwrap();
        assert_eq!(usage.resident_writers_root, 0);
        assert_eq!(usage.pending_generations, 0);
        assert_eq!(usage.inflight_calls, 0);
    }

    #[tokio::test]
    async fn passive_quiesce_nonempty_unsealed_replay_requires_fresh_claim_and_preserves_authority()
    {
        let key = StreamWorkerKey::new(
            TableIdentity::new(73, 79).unwrap(),
            ShardId::new_v4(),
            ShardId::new_v4(),
        )
        .unwrap();
        let (draining, claim) = passive_test_drain_and_claim(key, 9, 4, 2, false);
        validate_passive_drain_claim(key, &draining, &claim).unwrap();
        let registry = MemWalWorkerRegistry::for_root(
            &format!("memory://passive-replay/{}", ShardId::new_v4()),
            limits(),
        )
        .unwrap();
        let admission = Arc::new(tokio::sync::RwLock::new(()));
        let exclusive = Arc::clone(&admission).write_owned().await;
        let inflight = registry.reserve_inflight_core().unwrap();
        let disposition = registry
            .passive_disposition_from_physical(
                key,
                CheckedExclusiveStreamAuthority::from_exclusive_admission(exclusive),
                &draining,
                &claim,
                PassiveB1PhysicalState::AdmitOrReplay {
                    shard_manifest_version: 9,
                    current_generation: 1,
                    replay_after_wal_entry_position: 2,
                    writer_epoch: 4,
                },
                inflight,
            )
            .unwrap();
        let PassiveQuiesceDisposition::FreshClaimRequired(authority) = disposition else {
            panic!("nonempty unsealed replay must require one fresh claim");
        };
        assert!(
            tokio::time::timeout(
                Duration::from_millis(20),
                Arc::clone(&admission).read_owned()
            )
            .await
            .is_err(),
            "fresh-claim handoff must retain exclusive admission"
        );
        assert_eq!(registry.usage.lock().unwrap().inflight_calls, 0);
        drop(authority);
        tokio::time::timeout(Duration::from_secs(1), admission.read_owned())
            .await
            .expect("dropping returned authority must release admission");
    }

    #[tokio::test]
    async fn passive_quiesce_reuses_current_empty_claim_without_fresh_claim() {
        let key = StreamWorkerKey::new(
            TableIdentity::new(83, 89).unwrap(),
            ShardId::new_v4(),
            ShardId::new_v4(),
        )
        .unwrap();
        let (draining, claim) = passive_test_drain_and_claim(key, 11, 5, 1, true);
        validate_passive_drain_claim(key, &draining, &claim).unwrap();
        let registry = MemWalWorkerRegistry::for_root(
            &format!("memory://passive-empty/{}", ShardId::new_v4()),
            limits(),
        )
        .unwrap();
        let admission = Arc::new(tokio::sync::RwLock::new(()));
        let exclusive = Arc::clone(&admission).write_owned().await;
        let inflight = registry.reserve_inflight_core().unwrap();
        let disposition = registry
            .passive_disposition_from_physical(
                key,
                CheckedExclusiveStreamAuthority::from_exclusive_admission(exclusive),
                &draining,
                &claim,
                PassiveB1PhysicalState::AdmitOrReplay {
                    shard_manifest_version: 12,
                    current_generation: 1,
                    replay_after_wal_entry_position: 0,
                    writer_epoch: 5,
                },
                inflight,
            )
            .unwrap();
        let PassiveQuiesceDisposition::Reusable(QuiesceCut::Empty(cut)) = disposition else {
            panic!("an exact sentinel-only current claim must reuse its empty cut");
        };
        assert_eq!(cut.writer_epoch, 5);
        assert_eq!(cut.shard_manifest_version, 12);
        assert_eq!(cut.current_generation, 1);
        assert_eq!(cut.replay_after_wal_entry_position, 0);
        drop(cut.into_exclusive_authority());
        tokio::time::timeout(Duration::from_secs(1), admission.read_owned())
            .await
            .expect("dropping empty-cut authority must release admission");
        assert_eq!(registry.usage.lock().unwrap().inflight_calls, 0);
    }

    #[test]
    fn passive_quiesce_refuses_foreign_drain_epoch_and_cursor_authority() {
        let key = StreamWorkerKey::new(
            TableIdentity::new(97, 101).unwrap(),
            ShardId::new_v4(),
            ShardId::new_v4(),
        )
        .unwrap();
        let (draining, claim) = passive_test_drain_and_claim(key, 13, 6, 2, false);
        let foreign_key = StreamWorkerKey::new(
            TableIdentity::new(103, 107).unwrap(),
            key.enrollment_id,
            key.shard_id,
        )
        .unwrap();
        assert!(validate_passive_drain_claim(foreign_key, &draining, &claim).is_err());
        let mut future_target = draining.clone();
        *future_target
            .drain
            .as_mut()
            .unwrap()
            .target_epoch_floor_by_shard
            .get_mut(&key.shard_id.to_string())
            .unwrap() = claim.achieved_writer_epoch + 1;
        *future_target
            .epoch_floor_by_shard
            .get_mut(&key.shard_id.to_string())
            .unwrap() = claim.achieved_writer_epoch + 1;
        future_target.validate().unwrap();
        assert!(validate_passive_drain_claim(key, &future_target, &claim).is_err());
        assert!(validate_passive_claim_epoch(&claim, 13, 7).is_err());
        assert!(validate_passive_unflushed_claim_physical_cut(&claim, 13, 6, 1).is_err());
        validate_passive_unflushed_claim_physical_cut(&claim, 14, 6, 2).unwrap();
        validate_passive_flushed_claim_physical_cut(&claim, 14, 6, 2).unwrap();

        let (_, stock_empty) = passive_test_drain_and_claim(key, 15, 7, 1, true);
        validate_passive_unflushed_claim_physical_cut(&stock_empty, 15, 7, 0).unwrap();
        assert!(validate_passive_unflushed_claim_physical_cut(&stock_empty, 15, 7, 1).is_err());
        validate_passive_flushed_claim_physical_cut(&stock_empty, 16, 7, 1).unwrap();
        assert!(validate_passive_flushed_claim_physical_cut(&stock_empty, 16, 7, 0).is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cold_fold_open_timeout_retains_full_budget_and_uncancelled_opener() {
        let mut bounded = limits();
        bounded.max_resident_writers_root = 1;
        bounded.max_resident_writers_per_table = 1;
        bounded.max_reserved_arrow_bytes = B1_MAX_GENERATION_ARROW_BYTES;
        bounded.max_pending_generations = 1;
        bounded.seal_deadline = Duration::from_millis(25);
        let registry = MemWalWorkerRegistry::for_root(
            &format!("memory://fold-open-timeout/{}", ShardId::new_v4()),
            bounded,
        )
        .unwrap();
        let key = StreamWorkerKey::new(
            TableIdentity::new(47, 53).unwrap(),
            ShardId::new_v4(),
            ShardId::new_v4(),
        )
        .unwrap();
        let admission = Arc::new(tokio::sync::RwLock::new(()));
        let exclusive = Arc::clone(&admission).write_owned().await;
        let (settle, opening) = tokio::sync::oneshot::channel();
        let opener: WorkerOpener = Box::new(move || {
            Box::pin(async move {
                opening
                    .await
                    .expect("test controls the opener's settled result")
            })
        });

        let error = match Arc::clone(&registry)
            .seal_and_drain(
                key,
                "node:Test".to_string(),
                CheckedExclusiveStreamAuthority::from_exclusive_admission(exclusive),
                opener,
            )
            .await
        {
            Ok(_) => panic!("the caller-facing opener wait must honor seal_deadline"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("cold fold opener did not settle")
        );
        {
            let usage = registry.usage.lock().unwrap();
            assert_eq!(usage.resident_writers_root, 1);
            assert_eq!(usage.reserved_arrow_bytes, B1_MAX_GENERATION_ARROW_BYTES);
            assert_eq!(usage.pending_generations, 1);
            assert_eq!(usage.inflight_calls, 1);
            assert_eq!(
                usage.fold_required_by_key.get(&key),
                Some(&full_generation_reservation())
            );
        }
        assert!(
            tokio::time::timeout(
                Duration::from_millis(20),
                Arc::clone(&admission).read_owned(),
            )
            .await
            .is_err(),
            "timed-out opener continuation must retain exclusive admission"
        );

        assert!(
            settle
                .send(Err(WorkerOpenFailure::unclaimed(MemWalWorkerError::state(
                    "controlled unclaimed opener failure",
                ))))
                .is_ok(),
            "owned opener future must still be alive after caller timeout"
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let released = {
                    let usage = registry.usage.lock().unwrap();
                    usage.resident_writers_root == 0
                        && usage.reserved_arrow_bytes == 0
                        && usage.pending_generations == 0
                        && usage.inflight_calls == 0
                };
                if released {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("unclaimed settled opener must release every conservative reservation");
        tokio::time::timeout(Duration::from_secs(1), admission.write_owned())
            .await
            .expect("settled unclaimed opener must release exclusive admission");
    }

    #[tokio::test]
    async fn prepared_failure_retains_shared_authority_until_registry_consumes_it() {
        let admission = Arc::new(tokio::sync::RwLock::new(()));
        let shared = Arc::clone(&admission).read_owned().await;
        let failure = PreparedPutFailure::warm(
            OmniError::Lance("fresh authority changed".to_string()),
            CheckedStreamAuthority::from_shared_admission(shared),
        );

        assert!(
            tokio::time::timeout(
                Duration::from_millis(10),
                Arc::clone(&admission).write_owned(),
            )
            .await
            .is_err(),
            "failure carrier must retain the shared admission lease"
        );
        let (_error, authority, claimed) = failure.into_parts();
        assert!(claimed.is_none());
        assert!(
            tokio::time::timeout(
                Duration::from_millis(10),
                Arc::clone(&admission).write_owned(),
            )
            .await
            .is_err(),
            "destructuring must transfer rather than release authority"
        );
        drop(authority);
        tokio::time::timeout(Duration::from_secs(1), admission.write_owned())
            .await
            .expect("exclusive admission becomes available only after retirement owns/releases it");
    }

    #[test]
    fn lance_deadlines_bound_join_waiters_not_lance_futures() {
        let source = include_str!("worker.rs");
        let implementation = source.split("#[cfg(test)]").next().unwrap();
        assert!(implementation.contains("timeout_at(deadline, &mut invoked)"));
        assert!(implementation.contains("timeout_at(deadline, &mut watcher_task)"));
        assert!(implementation.contains("timeout_at(deadline, &mut fence_task)"));
        assert!(implementation.contains("timeout_at(deadline, &mut physical_cut)"));
        assert!(implementation.contains("timeout_at(deadline, &mut open_task)"));
        assert!(!implementation.contains("timeout_at(deadline, worker.writer.put_no_wait"));
        assert!(!implementation.contains("timeout_at(deadline, watcher.wait()"));
        assert!(!implementation.contains("timeout_at(deadline, worker.writer.check_fenced()"));
        assert!(!implementation.contains("timeout_at(deadline, physical_cut).await"));
        assert!(!implementation.contains("timeout_at(deadline, opener())"));
        assert!(implementation.contains("if let Ok(Ok((_, Some(mut watcher)))) = invoked.await"));
    }
}
