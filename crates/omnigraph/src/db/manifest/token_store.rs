//! Manifest-selected RFC-026 stream-token dataset authority.
//!
//! `_stream_tokens.lance` is a graph-global physical participant.  Its raw
//! Lance HEAD is never logical authority: `__manifest` stores one exact
//! [`CurrentHeadWitness`], and every reader opens that selected version and
//! verifies the complete witness before using it.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use std::str::FromStr;

use arrow_array::{Array, RecordBatch, RecordBatchIterator, StringArray, UInt32Array, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::{col, lit};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::refs::BranchIdentifier;
use lance::dataset::write::merge_insert::SourceDedupeBehavior;
use lance::dataset::{MergeInsertBuilder, WhenMatched, WhenNotMatched, WriteMode, WriteParams};
use lance::datatypes::LANCE_UNENFORCED_PRIMARY_KEY;
use lance_file::version::LanceFileVersion;
use lance_index::mem_wal::ShardId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{OmniError, Result};

use super::layout::{stream_token_authority_object_id, stream_token_uri};
use super::stream_token::{
    PayloadDigest, StreamRowOrigin, StreamTerminalCorrection, StreamToken, StreamTokenAuthorityRow,
    StreamTokenDisposition, TrustedContributorId,
};
use super::{CurrentHeadWitness, TableIdentity};

pub(crate) const STREAM_TOKEN_DATASET_PATH: &str = "_stream_tokens.lance";
pub(crate) const STREAM_TOKEN_AUTHORITY_SCHEMA_VERSION: u32 = 1;
const STREAM_TOKEN_AUTHORITY_PROTOCOL_VERSION: u32 = 1;

/// Canonical schema descriptor hashed into the manifest authority row.
///
/// Keep this in the same order and nullability as [`stream_token_schema`].  A
/// physical schema change requires a new descriptor, schema version, and graph
/// format strand; it must never be accepted through permissive field matching.
const STREAM_TOKEN_SCHEMA_DESCRIPTOR_V1: &str = concat!(
    "omnigraph.stream-token-authority.schema.v1\n",
    "id:utf8:required:unenforced-primary-key\n",
    "stable_table_id:uint64:required\n",
    "table_incarnation_id:uint64:required\n",
    "logical_id:utf8:required\n",
    "origin_enrollment_id:utf8:required\n",
    "stream_incarnation_id:utf8:required\n",
    "current_token:utf8:required\n",
    "write_id:utf8:required\n",
    "predecessor_token:utf8:nullable\n",
    "disposition:utf8:required\n",
    "contributor_id:utf8:required\n",
    "payload_digest:utf8:required\n",
    "origin_kind:utf8:required\n",
    "origin_id:utf8:required\n",
    "origin_ordinal:uint64:required\n",
    "fold_base_token:utf8:nullable\n",
    "chain_depth:uint32:required\n",
    "terminal_correction_actor:utf8:nullable\n",
    "terminal_correction_operation_id:utf8:nullable\n",
);

/// The single graph-global token-table pointer materialized in `__manifest`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StreamTokenAuthorityEntry {
    /// Canonical graph-relative dataset path.  This is never a user table path.
    pub(crate) location: String,
    pub(crate) schema_version: u32,
    pub(crate) schema_hash: String,
    /// Exact main-branch version selected by the same manifest snapshot.
    /// Its provider-local `manifest_e_tag` is always absent: a local graph
    /// copy changes inode-derived ETags without changing the immutable Lance
    /// version or transaction identity.
    pub(crate) current_head_witness: CurrentHeadWitness,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StreamTokenAuthorityPayload {
    protocol_version: u32,
    schema_version: u32,
    schema_hash: String,
    current_head_witness: CurrentHeadWitness,
}

impl StreamTokenAuthorityEntry {
    pub(crate) fn object_id(&self) -> &'static str {
        stream_token_authority_object_id()
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.location != STREAM_TOKEN_DATASET_PATH {
            return Err(OmniError::manifest_internal(format!(
                "stream-token authority location '{}' is not canonical '{}'",
                self.location, STREAM_TOKEN_DATASET_PATH
            )));
        }
        if self.schema_version != STREAM_TOKEN_AUTHORITY_SCHEMA_VERSION {
            return Err(OmniError::manifest_internal(format!(
                "unsupported stream-token authority schema version {}, expected {}",
                self.schema_version, STREAM_TOKEN_AUTHORITY_SCHEMA_VERSION
            )));
        }
        let expected_hash = stream_token_schema_hash();
        if self.schema_hash != expected_hash {
            return Err(OmniError::manifest_internal(format!(
                "stream-token authority schema hash '{}' does not match '{}'",
                self.schema_hash, expected_hash
            )));
        }
        validate_head_witness(&self.current_head_witness)
    }

    pub(super) fn to_metadata_json(&self) -> Result<String> {
        self.validate()?;
        serde_json::to_string(&StreamTokenAuthorityPayload {
            protocol_version: STREAM_TOKEN_AUTHORITY_PROTOCOL_VERSION,
            schema_version: self.schema_version,
            schema_hash: self.schema_hash.clone(),
            current_head_witness: self.current_head_witness.clone(),
        })
        .map_err(|error| {
            OmniError::manifest_internal(format!(
                "failed to encode stream-token authority metadata: {error}"
            ))
        })
    }

    pub(super) fn from_manifest_row(
        object_id: &str,
        location: Option<&str>,
        table_version: Option<u64>,
        table_branch: Option<&str>,
        metadata_json: &str,
    ) -> Result<Self> {
        let expected_object_id = stream_token_authority_object_id();
        if object_id != expected_object_id {
            return Err(OmniError::manifest_internal(format!(
                "manifest stream_token_authority row has object_id '{object_id}', expected '{expected_object_id}'"
            )));
        }
        let payload: StreamTokenAuthorityPayload =
            serde_json::from_str(metadata_json).map_err(|error| {
                OmniError::manifest_internal(format!(
                    "failed to decode stream-token authority metadata: {error}"
                ))
            })?;
        if payload.protocol_version != STREAM_TOKEN_AUTHORITY_PROTOCOL_VERSION {
            return Err(OmniError::manifest_internal(format!(
                "unsupported stream-token authority payload version {}, expected {}",
                payload.protocol_version, STREAM_TOKEN_AUTHORITY_PROTOCOL_VERSION
            )));
        }
        let location = location.ok_or_else(|| {
            OmniError::manifest_internal("manifest stream_token_authority row is missing location")
        })?;
        let entry = Self {
            location: location.to_string(),
            schema_version: payload.schema_version,
            schema_hash: payload.schema_hash,
            current_head_witness: payload.current_head_witness,
        };
        entry.validate()?;
        if table_version != Some(entry.current_head_witness.table_version) {
            return Err(OmniError::manifest_internal(
                "manifest stream_token_authority row table_version does not match its exact HEAD witness",
            ));
        }
        if table_branch.is_some() {
            return Err(OmniError::manifest_internal(
                "manifest stream_token_authority row must select canonical main (table_branch = null)",
            ));
        }
        Ok(entry)
    }
}

pub(crate) fn stream_token_schema() -> SchemaRef {
    let primary_key_metadata: HashMap<String, String> =
        [(LANCE_UNENFORCED_PRIMARY_KEY.to_string(), "true".to_string())]
            .into_iter()
            .collect();
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false).with_metadata(primary_key_metadata),
        Field::new("stable_table_id", DataType::UInt64, false),
        Field::new("table_incarnation_id", DataType::UInt64, false),
        Field::new("logical_id", DataType::Utf8, false),
        Field::new("origin_enrollment_id", DataType::Utf8, false),
        Field::new("stream_incarnation_id", DataType::Utf8, false),
        Field::new("current_token", DataType::Utf8, false),
        Field::new("write_id", DataType::Utf8, false),
        Field::new("predecessor_token", DataType::Utf8, true),
        Field::new("disposition", DataType::Utf8, false),
        Field::new("contributor_id", DataType::Utf8, false),
        Field::new("payload_digest", DataType::Utf8, false),
        Field::new("origin_kind", DataType::Utf8, false),
        Field::new("origin_id", DataType::Utf8, false),
        Field::new("origin_ordinal", DataType::UInt64, false),
        Field::new("fold_base_token", DataType::Utf8, true),
        Field::new("chain_depth", DataType::UInt32, false),
        Field::new("terminal_correction_actor", DataType::Utf8, true),
        Field::new("terminal_correction_operation_id", DataType::Utf8, true),
    ]))
}

pub(crate) fn stream_token_schema_hash() -> String {
    let digest = Sha256::digest(STREAM_TOKEN_SCHEMA_DESCRIPTOR_V1.as_bytes());
    format!("sha256:{digest:x}")
}

/// Collision-free canonical PK for `(table lifetime, logical id)`.
///
/// The table components are fixed-width hexadecimal and the complete UTF-8
/// logical id is the terminal component. Because it consumes the remainder of
/// the key (the identifier is never parsed by splitting from the right), even
/// embedded separators cannot alias another tuple. No zero/sentinel
/// [`TableIdentity`] is ever constructed for this graph-global dataset.
pub(crate) fn stream_token_row_id(identity: TableIdentity, logical_id: &str) -> Result<String> {
    identity.validate()?;
    Ok(format!(
        "stream-token-v1:{:016x}:{:016x}:{logical_id}",
        identity.stable_table_id, identity.table_incarnation_id,
    ))
}

/// Encode complete current-token rows using the exact v1 physical schema.
pub(crate) fn stream_token_rows_to_batch(rows: &[StreamTokenAuthorityRow]) -> Result<RecordBatch> {
    if rows.is_empty() {
        return Err(OmniError::manifest_internal(
            "stream-token upsert requires at least one authority row",
        ));
    }

    let mut ids = Vec::with_capacity(rows.len());
    let mut stable_table_ids = Vec::with_capacity(rows.len());
    let mut table_incarnation_ids = Vec::with_capacity(rows.len());
    let mut logical_ids = Vec::with_capacity(rows.len());
    let mut origin_enrollment_ids = Vec::with_capacity(rows.len());
    let mut stream_incarnation_ids = Vec::with_capacity(rows.len());
    let mut current_tokens = Vec::with_capacity(rows.len());
    let mut write_ids = Vec::with_capacity(rows.len());
    let mut predecessor_tokens = Vec::with_capacity(rows.len());
    let mut dispositions = Vec::with_capacity(rows.len());
    let mut contributors = Vec::with_capacity(rows.len());
    let mut payload_digests = Vec::with_capacity(rows.len());
    let mut origin_kinds = Vec::with_capacity(rows.len());
    let mut origin_ids = Vec::with_capacity(rows.len());
    let mut origin_ordinals = Vec::with_capacity(rows.len());
    let mut fold_base_tokens = Vec::with_capacity(rows.len());
    let mut chain_depths = Vec::with_capacity(rows.len());
    let mut terminal_actors = Vec::with_capacity(rows.len());
    let mut terminal_operation_ids = Vec::with_capacity(rows.len());
    let mut seen = std::collections::HashSet::with_capacity(rows.len());

    for row in rows {
        row.validate().map_err(stream_token_protocol_error)?;
        let id = stream_token_row_id(row.identity, &row.logical_id)?;
        if !seen.insert(id.clone()) {
            return Err(OmniError::manifest(format!(
                "stream-token upsert contains duplicate logical key ({}, '{}')",
                row.identity, row.logical_id
            )));
        }
        let (origin_kind, origin_id, origin_ordinal) = match &row.origin {
            StreamRowOrigin::Admission {
                admission_attempt_id,
                caller_ordinal,
            } => ("ADMISSION", admission_attempt_id.clone(), *caller_ordinal),
            StreamRowOrigin::Correction {
                correction_id,
                plan_ordinal,
            } => ("CORRECTION", correction_id.clone(), *plan_ordinal),
        };
        let (terminal_actor, terminal_operation_id) = row
            .terminal_correction
            .as_ref()
            .map(|correction| {
                (
                    Some(correction.actor.as_str().to_string()),
                    Some(correction.correction_id.clone()),
                )
            })
            .unwrap_or((None, None));

        ids.push(id);
        stable_table_ids.push(row.identity.stable_table_id);
        table_incarnation_ids.push(row.identity.table_incarnation_id);
        logical_ids.push(row.logical_id.clone());
        origin_enrollment_ids.push(row.origin_enrollment_id.clone());
        stream_incarnation_ids.push(row.stream_incarnation_id.clone());
        current_tokens.push(row.current_token.to_string());
        write_ids.push(row.write_id.clone());
        predecessor_tokens.push(row.predecessor_token.map(|token| token.to_string()));
        dispositions.push(match row.disposition {
            StreamTokenDisposition::Present => "PRESENT",
            StreamTokenDisposition::Withdrawn => "WITHDRAWN",
        });
        contributors.push(row.contributor_id.as_str().to_string());
        payload_digests.push(row.payload_digest.to_string());
        origin_kinds.push(origin_kind);
        origin_ids.push(origin_id);
        origin_ordinals.push(origin_ordinal);
        fold_base_tokens.push(row.fold_base_token.map(|token| token.to_string()));
        chain_depths.push(row.chain_depth);
        terminal_actors.push(terminal_actor);
        terminal_operation_ids.push(terminal_operation_id);
    }

    RecordBatch::try_new(
        stream_token_schema(),
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(UInt64Array::from(stable_table_ids)),
            Arc::new(UInt64Array::from(table_incarnation_ids)),
            Arc::new(StringArray::from(logical_ids)),
            Arc::new(StringArray::from(origin_enrollment_ids)),
            Arc::new(StringArray::from(stream_incarnation_ids)),
            Arc::new(StringArray::from(current_tokens)),
            Arc::new(StringArray::from(write_ids)),
            Arc::new(StringArray::from(predecessor_tokens)),
            Arc::new(StringArray::from(dispositions)),
            Arc::new(StringArray::from(contributors)),
            Arc::new(StringArray::from(payload_digests)),
            Arc::new(StringArray::from(origin_kinds)),
            Arc::new(StringArray::from(origin_ids)),
            Arc::new(UInt64Array::from(origin_ordinals)),
            Arc::new(StringArray::from(fold_base_tokens)),
            Arc::new(UInt32Array::from(chain_depths)),
            Arc::new(StringArray::from(terminal_actors)),
            Arc::new(StringArray::from(terminal_operation_ids)),
        ],
    )
    .map_err(|error| {
        OmniError::manifest_internal(format!(
            "failed to build stream-token authority batch: {error}"
        ))
    })
}

/// Enforce the config-v3 bounds for the exact winner projection which can
/// enter recovery-v12. This runs before acknowledgement for every projected
/// warm generation and again at the staging/recovery boundary.
pub(crate) fn validate_stream_token_plan_bounds(
    rows: &[StreamTokenAuthorityRow],
) -> Result<()> {
    validate_stream_token_plan_bounds_with_limits(
        rows,
        crate::table_store::mem_wal::B2_MAX_TOKEN_PROJECTION_ARROW_BYTES,
        crate::table_store::mem_wal::B2_MAX_TOKEN_RECOVERY_JSON_BYTES,
    )
}

pub(crate) fn add_stream_lookup_retained_bytes(
    resource: &'static str,
    current: u64,
    additional: u64,
    limit: u64,
) -> Result<u64> {
    let total = current.checked_add(additional).ok_or_else(|| {
        OmniError::manifest_internal(format!("{resource} retained-byte accounting overflow"))
    })?;
    if total > limit {
        return Err(OmniError::resource_limit(resource, limit, total));
    }
    Ok(total)
}

fn validate_stream_token_plan_bounds_with_limits(
    rows: &[StreamTokenAuthorityRow],
    arrow_limit: u64,
    json_limit: u64,
) -> Result<()> {
    let batch = stream_token_rows_to_batch(rows)?;
    let arrow_bytes = u64::try_from(batch.get_array_memory_size()).map_err(|_| {
        OmniError::manifest_internal("stream-token projection Arrow size exceeds u64")
    })?;
    if arrow_bytes > arrow_limit {
        return Err(OmniError::resource_limit(
            "stream_token_projection_arrow_bytes",
            arrow_limit,
            arrow_bytes,
        ));
    }

    let json_limit_usize = usize::try_from(json_limit)
    .map_err(|_| OmniError::manifest_internal("stream-token JSON cap exceeds usize"))?;
    let mut writer = BoundedCountWriter::new(json_limit_usize);
    let result = serde_json::to_writer(&mut writer, rows);
    if writer.exceeded {
        return Err(OmniError::resource_limit(
            "stream_token_recovery_json_bytes",
            json_limit,
            u64::try_from(writer.attempted).unwrap_or(u64::MAX),
        ));
    }
    result.map_err(|error| {
        OmniError::manifest_internal(format!(
            "failed to size stream-token recovery projection: {error}"
        ))
    })?;
    Ok(())
}

struct BoundedCountWriter {
    written: usize,
    limit: usize,
    attempted: usize,
    exceeded: bool,
}

impl BoundedCountWriter {
    fn new(limit: usize) -> Self {
        Self {
            written: 0,
            limit,
            attempted: 0,
            exceeded: false,
        }
    }
}

impl std::io::Write for BoundedCountWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.attempted = self.written.saturating_add(bytes.len());
        if self.attempted > self.limit {
            self.exceeded = true;
            return Err(std::io::Error::other(
                "stream-token recovery projection exceeds its configured byte cap",
            ));
        }
        self.written = self.attempted;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Decode and validate every current-token row from one exact-schema batch.
pub(crate) fn stream_token_rows_from_batch(
    batch: &RecordBatch,
) -> Result<Vec<StreamTokenAuthorityRow>> {
    if batch.schema().as_ref() != stream_token_schema().as_ref() {
        return Err(OmniError::manifest_internal(
            "stream-token scan returned a non-v1 physical schema",
        ));
    }
    let ids = required_string_array(batch, "id")?;
    let stable_table_ids = required_u64_array(batch, "stable_table_id")?;
    let table_incarnation_ids = required_u64_array(batch, "table_incarnation_id")?;
    let logical_ids = required_string_array(batch, "logical_id")?;
    let origin_enrollment_ids = required_string_array(batch, "origin_enrollment_id")?;
    let stream_incarnation_ids = required_string_array(batch, "stream_incarnation_id")?;
    let current_tokens = required_string_array(batch, "current_token")?;
    let write_ids = required_string_array(batch, "write_id")?;
    let predecessor_tokens = required_string_array(batch, "predecessor_token")?;
    let dispositions = required_string_array(batch, "disposition")?;
    let contributors = required_string_array(batch, "contributor_id")?;
    let payload_digests = required_string_array(batch, "payload_digest")?;
    let origin_kinds = required_string_array(batch, "origin_kind")?;
    let origin_ids = required_string_array(batch, "origin_id")?;
    let origin_ordinals = required_u64_array(batch, "origin_ordinal")?;
    let fold_base_tokens = required_string_array(batch, "fold_base_token")?;
    let chain_depths = required_u32_array(batch, "chain_depth")?;
    let terminal_actors = required_string_array(batch, "terminal_correction_actor")?;
    let terminal_operation_ids = required_string_array(batch, "terminal_correction_operation_id")?;

    let mut rows = Vec::with_capacity(batch.num_rows());
    let mut seen = std::collections::HashSet::with_capacity(batch.num_rows());
    for index in 0..batch.num_rows() {
        require_non_null(ids, index, "id")?;
        require_non_null(stable_table_ids, index, "stable_table_id")?;
        require_non_null(table_incarnation_ids, index, "table_incarnation_id")?;
        require_non_null(logical_ids, index, "logical_id")?;
        require_non_null(origin_enrollment_ids, index, "origin_enrollment_id")?;
        require_non_null(stream_incarnation_ids, index, "stream_incarnation_id")?;
        require_non_null(current_tokens, index, "current_token")?;
        require_non_null(write_ids, index, "write_id")?;
        require_non_null(dispositions, index, "disposition")?;
        require_non_null(contributors, index, "contributor_id")?;
        require_non_null(payload_digests, index, "payload_digest")?;
        require_non_null(origin_kinds, index, "origin_kind")?;
        require_non_null(origin_ids, index, "origin_id")?;
        require_non_null(origin_ordinals, index, "origin_ordinal")?;
        require_non_null(chain_depths, index, "chain_depth")?;

        let identity = TableIdentity::new(
            stable_table_ids.value(index),
            table_incarnation_ids.value(index),
        )?;
        let logical_id = logical_ids.value(index).to_string();
        let expected_id = stream_token_row_id(identity, &logical_id)?;
        if ids.value(index) != expected_id {
            return Err(OmniError::manifest_internal(format!(
                "stream-token row id '{}' does not match canonical key '{}'",
                ids.value(index),
                expected_id
            )));
        }
        if !seen.insert(expected_id) {
            return Err(OmniError::manifest_internal(format!(
                "stream-token batch contains duplicate logical key ({identity}, '{logical_id}')"
            )));
        }

        let origin = match origin_kinds.value(index) {
            "ADMISSION" => StreamRowOrigin::Admission {
                admission_attempt_id: origin_ids.value(index).to_string(),
                caller_ordinal: origin_ordinals.value(index),
            },
            "CORRECTION" => StreamRowOrigin::Correction {
                correction_id: origin_ids.value(index).to_string(),
                plan_ordinal: origin_ordinals.value(index),
            },
            other => {
                return Err(OmniError::manifest_internal(format!(
                    "stream-token row has unsupported origin_kind '{other}'"
                )));
            }
        };
        let disposition = match dispositions.value(index) {
            "PRESENT" => StreamTokenDisposition::Present,
            "WITHDRAWN" => StreamTokenDisposition::Withdrawn,
            other => {
                return Err(OmniError::manifest_internal(format!(
                    "stream-token row has unsupported disposition '{other}'"
                )));
            }
        };
        let terminal_correction = match (
            terminal_actors.is_null(index),
            terminal_operation_ids.is_null(index),
        ) {
            (true, true) => None,
            (false, false) => Some(StreamTerminalCorrection {
                actor: TrustedContributorId::new(terminal_actors.value(index).to_string())
                    .map_err(stream_token_protocol_error)?,
                correction_id: terminal_operation_ids.value(index).to_string(),
            }),
            _ => {
                return Err(OmniError::manifest_internal(
                    "stream-token terminal correction actor and operation must be both null or both present",
                ));
            }
        };
        let row = StreamTokenAuthorityRow {
            identity,
            logical_id,
            origin_enrollment_id: origin_enrollment_ids.value(index).to_string(),
            stream_incarnation_id: stream_incarnation_ids.value(index).to_string(),
            current_token: StreamToken::from_str(current_tokens.value(index))
                .map_err(stream_token_protocol_error)?,
            write_id: write_ids.value(index).to_string(),
            predecessor_token: optional_stream_token(predecessor_tokens, index)?,
            disposition,
            contributor_id: TrustedContributorId::new(contributors.value(index).to_string())
                .map_err(stream_token_protocol_error)?,
            payload_digest: PayloadDigest::from_str(payload_digests.value(index))
                .map_err(stream_token_protocol_error)?,
            origin,
            fold_base_token: optional_stream_token(fold_base_tokens, index)?,
            chain_depth: chain_depths.value(index),
            terminal_correction,
        };
        row.validate().map_err(stream_token_protocol_error)?;
        rows.push(row);
    }
    Ok(rows)
}

/// Look up one logical graph key from an already exact-pinned token dataset.
pub(crate) async fn lookup_stream_token_row(
    dataset: &Dataset,
    authority: &StreamTokenAuthorityEntry,
    identity: TableIdentity,
    logical_id: &str,
) -> Result<Option<StreamTokenAuthorityRow>> {
    validate_exact_dataset(dataset, authority).await?;
    let id = stream_token_row_id(identity, logical_id)?;
    let mut scanner = dataset.scan();
    scanner.filter_expr(col("id").eq(lit(id)));
    scanner.batch_size(2);
    scanner.batch_size_bytes(
        crate::table_store::mem_wal::B2_MAX_TOKEN_PROJECTION_ARROW_BYTES,
    );
    scanner
        .limit(Some(2), None)
        .map_err(|error| OmniError::Lance(error.to_string()))?;
    let mut stream = scanner
        .try_into_stream()
        .await
        .map_err(|error| OmniError::Lance(error.to_string()))?;
    let mut selected = None;
    while let Some(batch) = stream
        .try_next()
        .await
        .map_err(|error| OmniError::Lance(error.to_string()))?
    {
        let batch_bytes = u64::try_from(batch.get_array_memory_size()).map_err(|_| {
            OmniError::manifest_internal("stream-token lookup batch Arrow size exceeds u64")
        })?;
        if batch_bytes > crate::table_store::mem_wal::B2_MAX_TOKEN_PROJECTION_ARROW_BYTES {
            return Err(OmniError::resource_limit(
                "stream_token_lookup_batch_arrow_bytes",
                crate::table_store::mem_wal::B2_MAX_TOKEN_PROJECTION_ARROW_BYTES,
                batch_bytes,
            ));
        }
        for row in stream_token_rows_from_batch(&batch)? {
            let retained_bytes = row
                .lookup_retained_bytes()
                .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
            if retained_bytes
                > crate::table_store::mem_wal::B2_MAX_TOKEN_PROJECTION_ARROW_BYTES
            {
                return Err(OmniError::resource_limit(
                    "stream_token_lookup_retained_bytes",
                    crate::table_store::mem_wal::B2_MAX_TOKEN_PROJECTION_ARROW_BYTES,
                    retained_bytes,
                ));
            }
            if selected.replace(row).is_some() {
                return Err(OmniError::manifest_internal(format!(
                    "manifest-selected stream-token dataset contains duplicate current rows for ({identity}, '{logical_id}')"
                )));
            }
        }
    }
    if selected
        .as_ref()
        .is_some_and(|row| row.identity != identity || row.logical_id != logical_id)
    {
        return Err(OmniError::manifest_internal(
            "stream-token exact-id lookup returned a row for a different logical key",
        ));
    }
    Ok(selected)
}

/// Read only the manifest-selected current-token rows named by one bounded
/// generation. The structured exact-id predicate keeps materialized output
/// bounded by the generation instead of by all retained token authority.
pub(crate) async fn stream_token_rows_for_keys(
    dataset: &Dataset,
    authority: &StreamTokenAuthorityEntry,
    identity: TableIdentity,
    logical_ids: &std::collections::BTreeSet<String>,
) -> Result<BTreeMap<String, StreamTokenAuthorityRow>> {
    validate_exact_dataset(dataset, authority).await?;
    if logical_ids.is_empty()
        || logical_ids.len()
            > crate::table_store::mem_wal::B1_MAX_GENERATION_ROWS as usize
    {
        return Err(OmniError::manifest_internal(format!(
            "stream-token fold lookup requires 1..={} exact keys, got {}",
            crate::table_store::mem_wal::B1_MAX_GENERATION_ROWS,
            logical_ids.len()
        )));
    }
    let exact_ids = logical_ids
        .iter()
        .map(|logical_id| stream_token_row_id(identity, logical_id))
        .collect::<Result<Vec<_>>>()?;
    let mut scanner = dataset.scan();
    scanner.filter_expr(col("id").in_list(exact_ids.into_iter().map(lit).collect(), false));
    scanner.batch_size(logical_ids.len().saturating_add(1));
    scanner.batch_size_bytes(
        crate::table_store::mem_wal::B2_MAX_TOKEN_PROJECTION_ARROW_BYTES,
    );
    scanner
        .limit(
            Some(i64::try_from(logical_ids.len().saturating_add(1)).map_err(|_| {
                OmniError::manifest_internal("stream-token lookup row limit exceeds i64")
            })?),
            None,
        )
        .map_err(|error| OmniError::Lance(error.to_string()))?;
    let mut stream = scanner
        .try_into_stream()
        .await
        .map_err(|error| OmniError::Lance(error.to_string()))?;
    let mut selected = BTreeMap::new();
    let mut observed_rows = 0_usize;
    let mut retained_bytes = 0_u64;
    while let Some(batch) = stream
        .try_next()
        .await
        .map_err(|error| OmniError::Lance(error.to_string()))?
    {
        observed_rows = observed_rows
            .checked_add(batch.num_rows())
            .ok_or_else(|| OmniError::manifest_internal("stream-token lookup row overflow"))?;
        if observed_rows > logical_ids.len() {
            return Err(OmniError::manifest_internal(format!(
                "manifest-selected stream-token dataset returned more than one row per requested key for table {identity}"
            )));
        }
        let batch_bytes = u64::try_from(batch.get_array_memory_size()).map_err(|_| {
            OmniError::manifest_internal("stream-token lookup batch Arrow size exceeds u64")
        })?;
        if batch_bytes > crate::table_store::mem_wal::B2_MAX_TOKEN_PROJECTION_ARROW_BYTES {
            return Err(OmniError::resource_limit(
                "stream_token_lookup_batch_arrow_bytes",
                crate::table_store::mem_wal::B2_MAX_TOKEN_PROJECTION_ARROW_BYTES,
                batch_bytes,
            ));
        }
        for row in stream_token_rows_from_batch(&batch)? {
            if row.identity != identity || !logical_ids.contains(&row.logical_id) {
                return Err(OmniError::manifest_internal(
                    "stream-token exact-key scan returned a row outside its requested key set",
                ));
            }
            retained_bytes = add_stream_lookup_retained_bytes(
                "stream_token_lookup_retained_bytes",
                retained_bytes,
                row.lookup_retained_bytes()
                    .map_err(|error| OmniError::manifest_internal(error.to_string()))?,
                crate::table_store::mem_wal::B2_MAX_TOKEN_PROJECTION_ARROW_BYTES,
            )?;
            if selected.insert(row.logical_id.clone(), row).is_some() {
                return Err(OmniError::manifest_internal(format!(
                    "manifest-selected stream-token dataset contains duplicate current rows for table {identity}"
                )));
            }
        }
    }
    Ok(selected)
}

/// Stage one exact-`id` current-token upsert without advancing Lance HEAD.
///
/// `dataset` must be the exact handle selected by `authority`; the helper
/// validates that witness before producing any staged files. The returned
/// [`crate::table_store::StagedWrite`] must enter recovery-v12 before its one
/// strict `commit_staged_exact` invocation.
pub(crate) async fn stage_stream_token_upsert(
    dataset: Dataset,
    authority: &StreamTokenAuthorityEntry,
    rows: &[StreamTokenAuthorityRow],
) -> Result<crate::table_store::StagedWrite> {
    validate_exact_dataset(&dataset, authority).await?;
    validate_stream_token_plan_bounds(rows)?;
    let batch = stream_token_rows_to_batch(rows)?;
    let row_count = u64::try_from(batch.num_rows())
        .map_err(|_| OmniError::manifest_internal("stream-token upsert row count exceeds u64"))?;
    let schema = batch.schema();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let stream = lance_datafusion::utils::reader_to_stream(Box::new(reader));
    let mut builder =
        MergeInsertBuilder::try_new(Arc::new(dataset.clone()), vec!["id".to_string()])
            .map_err(|error| OmniError::Lance(error.to_string()))?;
    builder
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .use_index(false)
        .conflict_retries(0)
        .source_dedupe_behavior(SourceDedupeBehavior::FirstSeen);
    let uncommitted = builder
        .try_build()
        .map_err(|error| OmniError::Lance(error.to_string()))?
        .execute_uncommitted(stream)
        .await
        .map_err(|error| OmniError::Lance(error.to_string()))?;
    if uncommitted.transaction.read_version != authority.current_head_witness.table_version {
        return Err(OmniError::manifest_internal(format!(
            "stream-token staged transaction read version {} does not match manifest-selected version {}",
            uncommitted.transaction.read_version, authority.current_head_witness.table_version
        )));
    }
    crate::table_store::staged_exact_id_upsert_result(
        &dataset,
        uncommitted,
        row_count,
        "stage_stream_token_upsert",
    )
}

fn required_string_array<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "stream-token batch column '{name}' is missing or not Utf8"
            ))
        })
}

fn required_u64_array<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a UInt64Array> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "stream-token batch column '{name}' is missing or not UInt64"
            ))
        })
}

fn required_u32_array<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a UInt32Array> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<UInt32Array>())
        .ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "stream-token batch column '{name}' is missing or not UInt32"
            ))
        })
}

fn require_non_null(array: &dyn Array, index: usize, name: &str) -> Result<()> {
    if array.is_null(index) {
        return Err(OmniError::manifest_internal(format!(
            "stream-token required column '{name}' is null at row {index}"
        )));
    }
    Ok(())
}

fn optional_stream_token(array: &StringArray, index: usize) -> Result<Option<StreamToken>> {
    (!array.is_null(index))
        .then(|| StreamToken::from_str(array.value(index)).map_err(stream_token_protocol_error))
        .transpose()
}

fn stream_token_protocol_error(error: impl std::fmt::Display) -> OmniError {
    OmniError::manifest_internal(format!("invalid stream-token authority row: {error}"))
}

pub(super) async fn initialize_stream_token_authority(
    root_uri: &str,
    control_session: &Arc<lance::session::Session>,
) -> Result<StreamTokenAuthorityEntry> {
    let schema = stream_token_schema();
    let batch = RecordBatch::new_empty(schema.clone());
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let params = WriteParams {
        mode: WriteMode::Create,
        enable_stable_row_ids: true,
        data_storage_version: Some(LanceFileVersion::V2_2),
        auto_cleanup: None,
        skip_auto_cleanup: true,
        session: Some(Arc::clone(control_session)),
        ..Default::default()
    };
    let dataset = Dataset::write(reader, &stream_token_uri(root_uri), Some(params))
        .await
        .map_err(|error| OmniError::Lance(error.to_string()))?;
    stream_token_authority_entry_for_dataset(&dataset).await
}

/// Build the only valid manifest pointer for an already-achieved exact token
/// dataset version. Callers still need recovery ownership before any effect;
/// this helper only captures and validates the physical witness.
pub(crate) async fn stream_token_authority_entry_for_dataset(
    dataset: &Dataset,
) -> Result<StreamTokenAuthorityEntry> {
    let actual_schema: Schema = dataset.schema().into();
    if &actual_schema != stream_token_schema().as_ref() {
        return Err(OmniError::manifest_internal(
            "cannot publish a stream-token dataset with a non-v1 schema",
        ));
    }
    let entry = StreamTokenAuthorityEntry {
        location: STREAM_TOKEN_DATASET_PATH.to_string(),
        schema_version: STREAM_TOKEN_AUTHORITY_SCHEMA_VERSION,
        schema_hash: stream_token_schema_hash(),
        current_head_witness: capture_exact_head_witness(dataset).await?,
    };
    entry.validate()?;
    Ok(entry)
}

/// Open only the exact token-table version selected by `__manifest`.
///
/// This helper intentionally has no latest-HEAD fallback.  A moved raw HEAD is
/// invisible until its exact witness is published, and any mismatch at the
/// selected version is corruption rather than an adoption opportunity.
pub(crate) async fn open_stream_token_authority_at(
    root_uri: &str,
    authority: &StreamTokenAuthorityEntry,
    control_session: &Arc<lance::session::Session>,
) -> Result<Dataset> {
    authority.validate()?;
    let dataset = crate::instrumentation::open_dataset(
        &stream_token_uri(root_uri),
        crate::instrumentation::VersionResolution::At(authority.current_head_witness.table_version),
        Some(control_session),
        crate::instrumentation::table_wrapper(),
    )
    .await?;
    validate_exact_dataset(&dataset, authority).await?;
    Ok(dataset)
}

/// Open raw token HEAD only as a final uncovered-drift check. The caller must
/// already hold the graph-global stream-token gate and compare this complete
/// witness with manifest authority before arming any other participant.
pub(crate) async fn open_stream_token_authority_head(
    root_uri: &str,
    expected: &StreamTokenAuthorityEntry,
    control_session: &Arc<lance::session::Session>,
) -> Result<Dataset> {
    expected.validate()?;
    let dataset = crate::instrumentation::open_dataset(
        &stream_token_uri(root_uri),
        crate::instrumentation::VersionResolution::Latest,
        Some(control_session),
        crate::instrumentation::table_wrapper(),
    )
    .await?;
    let observed = stream_token_authority_entry_for_dataset(&dataset).await?;
    if &observed != expected {
        return Err(OmniError::manifest_conflict(format!(
            "stream-token raw HEAD {:?} differs from manifest-selected authority {:?}; explicit recovery is required",
            observed.current_head_witness, expected.current_head_witness
        )));
    }
    Ok(dataset)
}

async fn validate_exact_dataset(
    dataset: &Dataset,
    authority: &StreamTokenAuthorityEntry,
) -> Result<()> {
    let actual_schema: Schema = dataset.schema().into();
    if &actual_schema != stream_token_schema().as_ref() {
        return Err(OmniError::manifest_internal(
            "manifest-selected stream-token dataset has a schema different from its v1 authority",
        ));
    }
    let actual = capture_exact_head_witness(dataset).await?;
    if actual != authority.current_head_witness {
        return Err(OmniError::manifest_read_set_changed(
            stream_token_authority_object_id(),
            Some(authority.to_metadata_json()?),
            Some(
                StreamTokenAuthorityEntry {
                    location: authority.location.clone(),
                    schema_version: authority.schema_version,
                    schema_hash: authority.schema_hash.clone(),
                    current_head_witness: actual,
                }
                .to_metadata_json()?,
            ),
        ));
    }
    Ok(())
}

async fn capture_exact_head_witness(dataset: &Dataset) -> Result<CurrentHeadWitness> {
    let branch_before = dataset
        .branch_identifier()
        .await
        .map_err(|error| OmniError::Lance(error.to_string()))?;
    let table_version = dataset.version().version;
    let transaction = dataset
        .read_transaction()
        .await
        .map_err(|error| OmniError::Lance(error.to_string()))?
        .ok_or_else(|| {
            OmniError::manifest_internal(
                "manifest-selected stream-token dataset version has no transaction",
            )
        })?;
    let branch_after = dataset
        .branch_identifier()
        .await
        .map_err(|error| OmniError::Lance(error.to_string()))?;
    if branch_before != branch_after || dataset.version().version != table_version {
        return Err(OmniError::manifest_internal(
            "stream-token exact-version witness is internally incoherent",
        ));
    }
    let witness = CurrentHeadWitness {
        branch_identifier: branch_before,
        table_version,
        transaction_uuid: transaction.uuid,
        // Object-store ETags are useful within one provider commit attempt,
        // but they are not durable graph identity. In particular, LocalFileSystem
        // derives them partly from the inode, so copying an otherwise exact graph
        // changes the value. Version + Lance transaction UUID is the stable exact
        // token-table witness; strict commit/recovery still fences every effect.
        manifest_e_tag: None,
    };
    validate_head_witness(&witness)?;
    Ok(witness)
}

fn validate_head_witness(witness: &CurrentHeadWitness) -> Result<()> {
    if witness.branch_identifier != BranchIdentifier::main() {
        return Err(OmniError::manifest_internal(
            "stream-token authority must select the main Lance branch",
        ));
    }
    if witness.table_version == 0 {
        return Err(OmniError::manifest_internal(
            "stream-token authority table_version must be non-zero",
        ));
    }
    let transaction_uuid = ShardId::parse_str(&witness.transaction_uuid).map_err(|error| {
        OmniError::manifest_internal(format!(
            "stream-token authority transaction_uuid is not a UUID: {error}"
        ))
    })?;
    if transaction_uuid.is_nil() || transaction_uuid.to_string() != witness.transaction_uuid {
        return Err(OmniError::manifest_internal(
            "stream-token authority transaction_uuid must be non-nil canonical lowercase UUID text",
        ));
    }
    if witness.manifest_e_tag.is_some() {
        return Err(OmniError::manifest_internal(
            "stream-token authority manifest_e_tag must be absent because provider-local ETags are not durable graph identity",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::manifest::stream_token::{
        PayloadDigest, StreamRowOrigin, StreamTokenInput, TrustedContributorId,
    };

    fn authority_row_for(logical_id: &str, contributor: &str) -> StreamTokenAuthorityRow {
        let identity = TableIdentity::new(7, 9).unwrap();
        let contributor_id = TrustedContributorId::new(contributor).unwrap();
        let payload_digest = PayloadDigest::from_bytes([0x5a; 32]);
        let stream_incarnation_id = "11111111-1111-4111-8111-111111111111";
        let write_id = "22222222-2222-4222-8222-222222222222";
        let current_token = StreamToken::derive(&StreamTokenInput {
            identity,
            logical_id,
            stream_incarnation_id,
            predecessor_token: None,
            write_id,
            contributor_id: &contributor_id,
            payload_digest,
        })
        .unwrap();
        StreamTokenAuthorityRow {
            identity,
            logical_id: logical_id.to_string(),
            origin_enrollment_id: "33333333-3333-4333-8333-333333333333".to_string(),
            stream_incarnation_id: stream_incarnation_id.to_string(),
            current_token,
            write_id: write_id.to_string(),
            predecessor_token: None,
            disposition: StreamTokenDisposition::Present,
            contributor_id,
            payload_digest,
            origin: StreamRowOrigin::Admission {
                admission_attempt_id: "44444444-4444-4444-8444-444444444444".to_string(),
                caller_ordinal: 17,
            },
            fold_base_token: None,
            chain_depth: 1,
            terminal_correction: None,
        }
    }

    fn authority_row() -> StreamTokenAuthorityRow {
        authority_row_for("person:17", "actor:alice")
    }

    #[test]
    fn token_authority_row_batch_round_trips_exactly() {
        let row = authority_row();
        let batch = stream_token_rows_to_batch(std::slice::from_ref(&row)).unwrap();
        assert_eq!(stream_token_rows_from_batch(&batch).unwrap(), vec![row]);
    }

    #[test]
    fn token_plan_bounds_fail_loudly_for_arrow_and_recovery_json() {
        let row = authority_row();
        let arrow_error = validate_stream_token_plan_bounds_with_limits(
            std::slice::from_ref(&row),
            1,
            u64::MAX,
        )
        .unwrap_err();
        assert!(
            matches!(
                arrow_error,
                OmniError::ResourceLimitExceeded {
                    ref resource,
                    limit: 1,
                    actual,
                } if resource == "stream_token_projection_arrow_bytes" && actual > 1
            ),
            "{arrow_error:?}"
        );

        let json_error = validate_stream_token_plan_bounds_with_limits(
            std::slice::from_ref(&row),
            u64::MAX,
            1,
        )
        .unwrap_err();
        assert!(
            matches!(
                json_error,
                OmniError::ResourceLimitExceeded {
                    ref resource,
                    limit: 1,
                    actual,
                } if resource == "stream_token_recovery_json_bytes" && actual > 1
            ),
            "{json_error:?}"
        );
    }

    #[test]
    fn exact_lookup_retained_bytes_are_cumulative_and_fail_loudly() {
        let first = authority_row_for("person:17", "actor:alice");
        let second = authority_row_for("person:18", "actor:bob");
        first.validate().unwrap();
        second.validate().unwrap();
        let first_bytes = first.lookup_retained_bytes().unwrap();
        let second_bytes = second.lookup_retained_bytes().unwrap();
        let limit = first_bytes
            .checked_add(second_bytes)
            .unwrap()
            .checked_sub(1)
            .unwrap();
        let retained = add_stream_lookup_retained_bytes(
            "stream_token_lookup_retained_bytes",
            0,
            first_bytes,
            limit,
        )
        .unwrap();
        let error = add_stream_lookup_retained_bytes(
            "stream_token_lookup_retained_bytes",
            retained,
            second_bytes,
            limit,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            OmniError::ResourceLimitExceeded {
                ref resource,
                limit: actual_limit,
                actual,
            } if resource == "stream_token_lookup_retained_bytes"
                && actual_limit == limit
                && actual == first_bytes + second_bytes
        ));
    }

    #[tokio::test]
    async fn staged_token_upsert_is_invisible_until_commit_and_lookup_is_manifest_pinned() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let session = crate::lance_access::control_session();
        let authority = initialize_stream_token_authority(root, &session)
            .await
            .unwrap();
        assert!(
            authority.current_head_witness.manifest_e_tag.is_none(),
            "manifest-selected token authority must not persist provider-local ETags"
        );
        let dataset = open_stream_token_authority_at(root, &authority, &session)
            .await
            .unwrap();
        let row = authority_row();
        let staged =
            stage_stream_token_upsert(dataset.clone(), &authority, std::slice::from_ref(&row))
                .await
                .unwrap();
        assert_eq!(dataset.count_rows(None).await.unwrap(), 0);

        let store = crate::table_store::TableStore::new(root, Arc::clone(&session));
        let (achieved, committed) = store
            .commit_staged_exact(Arc::new(dataset), staged)
            .await
            .unwrap();
        assert_eq!(
            committed.read_version,
            authority.current_head_witness.table_version
        );
        let next = stream_token_authority_entry_for_dataset(&achieved)
            .await
            .unwrap();
        assert!(next.current_head_witness.manifest_e_tag.is_none());
        assert_eq!(
            lookup_stream_token_row(&achieved, &next, row.identity, &row.logical_id)
                .await
                .unwrap(),
            Some(row)
        );
    }
}
