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
use lance::dataset::scanner::DatasetRecordBatchStream;
use lance::dataset::write::merge_insert::SourceDedupeBehavior;
use lance::dataset::{MergeInsertBuilder, WhenMatched, WhenNotMatched, WriteMode, WriteParams};
use lance::datatypes::LANCE_UNENFORCED_PRIMARY_KEY;
use lance::index::DatasetIndexExt;
use lance_file::version::LanceFileVersion;
use lance_index::IndexType;
use lance_index::mem_wal::ShardId;
use lance_index::scalar::ScalarIndexParams;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{OmniError, Result};

use super::layout::{stream_token_authority_object_id, stream_token_uri};
use super::stream::{
    BINDING_RECEIPT_TAG, BindingReceipt, CLAIM_ATTEMPT_EFFECT_TAG, CLAIM_RECEIPT_TAG,
    ClaimAttemptEffect, ClaimReceipt, ENROLLMENT_RECEIPT_V2_TAG, EnrollmentReceiptV2,
    MANAGEMENT_RECEIPT_TAG, ManagementReceipt, STREAM_CORRECTION_RECEIPT_TAG,
    StreamCorrectionReceipt,
};
use super::stream_profile::{
    AUTHORITY_RETIREMENT_RECEIPT_TAG, AuthorityRetirementReceipt, PROFILE_MANAGEMENT_RECEIPT_TAG,
    ProfileManagementReceipt,
};
use super::stream_token::{
    AUTHORITY_RETIREMENT_RECEIPT_V2_TAG, AuthorityRetirementReceiptV2, PayloadDigest,
    StreamDeadLetterTerminalEvidence, StreamRowOrigin, StreamTerminalCorrection, StreamToken,
    StreamTokenAuthorityRow, StreamTokenDisposition, TrustedContributorId,
};
use super::{CurrentHeadWitness, TableIdentity};

pub(crate) const STREAM_TOKEN_DATASET_PATH: &str = "_stream_tokens.lance";
pub(crate) const STREAM_TOKEN_AUTHORITY_SCHEMA_VERSION: u32 = 3;
const STREAM_TOKEN_AUTHORITY_PROTOCOL_VERSION: u32 = 1;
pub(crate) const CURRENT_TOKEN_RECORD_TAG: &str = "CURRENT_TOKEN_V2";
const MAX_LIFECYCLE_LEDGER_RECORDS_PER_TRANSACTION: usize = 8;
const MAX_LIFECYCLE_LEDGER_RECORD_JSON_BYTES: usize = 16 * 1024;
const MAX_LIFECYCLE_LEDGER_TRANSACTION_JSON_BYTES: usize =
    MAX_LIFECYCLE_LEDGER_RECORDS_PER_TRANSACTION * MAX_LIFECYCLE_LEDGER_RECORD_JSON_BYTES;
const MAX_LIFECYCLE_LEDGER_TRANSACTION_ARROW_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
struct LifecycleLedgerEnvelope {
    record_id: String,
    record_tag: String,
    record_lookup_key: String,
    record_payload_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LifecycleLedgerRecord {
    EnrollmentReceiptV2(EnrollmentReceiptV2),
    BindingReceipt(BindingReceipt),
    ManagementReceipt(ManagementReceipt),
    StreamCorrectionReceipt(StreamCorrectionReceipt),
    ClaimAttemptEffect(ClaimAttemptEffect),
    ClaimReceipt(ClaimReceipt),
    AuthorityRetirementReceipt(AuthorityRetirementReceipt),
    AuthorityRetirementReceiptV2(AuthorityRetirementReceiptV2),
}

impl LifecycleLedgerRecord {
    pub(crate) fn record_id(&self) -> &str {
        match self {
            Self::EnrollmentReceiptV2(value) => &value.record_id,
            Self::BindingReceipt(value) => &value.record_id,
            Self::ManagementReceipt(value) => &value.record_id,
            Self::StreamCorrectionReceipt(value) => &value.record_id,
            Self::ClaimAttemptEffect(value) => &value.record_id,
            Self::ClaimReceipt(value) => &value.record_id,
            Self::AuthorityRetirementReceipt(value) => &value.record_id,
            Self::AuthorityRetirementReceiptV2(value) => &value.record_id,
        }
    }

    pub(crate) fn record_tag(&self) -> &'static str {
        match self {
            Self::EnrollmentReceiptV2(_) => ENROLLMENT_RECEIPT_V2_TAG,
            Self::BindingReceipt(_) => BINDING_RECEIPT_TAG,
            Self::ManagementReceipt(_) => MANAGEMENT_RECEIPT_TAG,
            Self::StreamCorrectionReceipt(_) => STREAM_CORRECTION_RECEIPT_TAG,
            Self::ClaimAttemptEffect(_) => CLAIM_ATTEMPT_EFFECT_TAG,
            Self::ClaimReceipt(_) => CLAIM_RECEIPT_TAG,
            Self::AuthorityRetirementReceipt(_) => AUTHORITY_RETIREMENT_RECEIPT_TAG,
            Self::AuthorityRetirementReceiptV2(_) => AUTHORITY_RETIREMENT_RECEIPT_V2_TAG,
        }
    }

    pub(crate) fn record_lookup_key(&self) -> &str {
        match self {
            Self::EnrollmentReceiptV2(value) => &value.record_lookup_key,
            Self::BindingReceipt(value) => &value.record_lookup_key,
            Self::ManagementReceipt(value) => &value.record_lookup_key,
            Self::StreamCorrectionReceipt(value) => &value.record_lookup_key,
            Self::ClaimAttemptEffect(value) => &value.record_lookup_key,
            Self::ClaimReceipt(value) => &value.record_lookup_key,
            Self::AuthorityRetirementReceipt(value) => &value.record_lookup_key,
            Self::AuthorityRetirementReceiptV2(value) => &value.record_lookup_key,
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        match self {
            Self::EnrollmentReceiptV2(value) => value.validate(),
            Self::BindingReceipt(value) => value.validate(),
            Self::ManagementReceipt(value) => value.validate(value.to_revision),
            Self::StreamCorrectionReceipt(value) => value.validate(),
            Self::ClaimAttemptEffect(value) => value.validate(),
            Self::ClaimReceipt(value) => value.validate(),
            Self::AuthorityRetirementReceipt(value) => value.validate(),
            Self::AuthorityRetirementReceiptV2(value) => {
                value.validate().map_err(stream_token_protocol_error)
            }
        }
    }

    fn to_envelope(&self) -> Result<LifecycleLedgerEnvelope> {
        self.validate()?;
        let record_payload_json = match self {
            Self::EnrollmentReceiptV2(value) => serde_json::to_string(value),
            Self::BindingReceipt(value) => serde_json::to_string(value),
            Self::ManagementReceipt(value) => serde_json::to_string(value),
            Self::StreamCorrectionReceipt(value) => serde_json::to_string(value),
            Self::ClaimAttemptEffect(value) => serde_json::to_string(value),
            Self::ClaimReceipt(value) => serde_json::to_string(value),
            Self::AuthorityRetirementReceipt(value) => serde_json::to_string(value),
            Self::AuthorityRetirementReceiptV2(value) => serde_json::to_string(value),
        }
        .map_err(|error| {
            OmniError::manifest_internal(format!(
                "failed to encode lifecycle ledger record: {error}"
            ))
        })?;
        Ok(LifecycleLedgerEnvelope {
            record_id: self.record_id().to_string(),
            record_tag: self.record_tag().to_string(),
            record_lookup_key: self.record_lookup_key().to_string(),
            record_payload_json,
        })
    }

    fn from_envelope(envelope: LifecycleLedgerEnvelope) -> Result<Self> {
        let record = match envelope.record_tag.as_str() {
            ENROLLMENT_RECEIPT_V2_TAG => Self::EnrollmentReceiptV2(
                serde_json::from_str(&envelope.record_payload_json).map_err(|error| {
                    OmniError::manifest_internal(format!(
                        "failed to decode lifecycle enrollment receipt: {error}"
                    ))
                })?,
            ),
            BINDING_RECEIPT_TAG => Self::BindingReceipt(
                serde_json::from_str(&envelope.record_payload_json).map_err(|error| {
                    OmniError::manifest_internal(format!(
                        "failed to decode lifecycle binding receipt: {error}"
                    ))
                })?,
            ),
            MANAGEMENT_RECEIPT_TAG => Self::ManagementReceipt(
                serde_json::from_str(&envelope.record_payload_json).map_err(|error| {
                    OmniError::manifest_internal(format!(
                        "failed to decode lifecycle management receipt: {error}"
                    ))
                })?,
            ),
            STREAM_CORRECTION_RECEIPT_TAG => Self::StreamCorrectionReceipt(
                serde_json::from_str(&envelope.record_payload_json).map_err(|error| {
                    OmniError::manifest_internal(format!(
                        "failed to decode lifecycle correction receipt: {error}"
                    ))
                })?,
            ),
            CLAIM_ATTEMPT_EFFECT_TAG => Self::ClaimAttemptEffect(
                serde_json::from_str(&envelope.record_payload_json).map_err(|error| {
                    OmniError::manifest_internal(format!(
                        "failed to decode lifecycle claim-attempt effect: {error}"
                    ))
                })?,
            ),
            CLAIM_RECEIPT_TAG => Self::ClaimReceipt(
                serde_json::from_str(&envelope.record_payload_json).map_err(|error| {
                    OmniError::manifest_internal(format!(
                        "failed to decode lifecycle claim receipt: {error}"
                    ))
                })?,
            ),
            AUTHORITY_RETIREMENT_RECEIPT_TAG => Self::AuthorityRetirementReceipt(
                serde_json::from_str(&envelope.record_payload_json).map_err(|error| {
                    OmniError::manifest_internal(format!(
                        "failed to decode authority-retirement receipt: {error}"
                    ))
                })?,
            ),
            AUTHORITY_RETIREMENT_RECEIPT_V2_TAG => Self::AuthorityRetirementReceiptV2(
                serde_json::from_str(&envelope.record_payload_json).map_err(|error| {
                    OmniError::manifest_internal(format!(
                        "failed to decode authority-retirement-v2 receipt: {error}"
                    ))
                })?,
            ),
            other => {
                return Err(OmniError::manifest_internal(format!(
                    "lifecycle ledger decoder received unsupported trusted record tag '{other}'"
                )));
            }
        };
        record.validate()?;
        if envelope.record_id != record.record_id()
            || envelope.record_tag != record.record_tag()
            || envelope.record_lookup_key != record.record_lookup_key()
        {
            return Err(OmniError::manifest_internal(
                "lifecycle ledger physical envelope differs from its canonical payload",
            ));
        }
        Ok(record)
    }
}

/// Canonical schema descriptor hashed into the manifest authority row.
///
/// Keep this in the same order and nullability as [`stream_token_schema`].  A
/// physical schema change requires a new descriptor, schema version, and graph
/// format strand; it must never be accepted through permissive field matching.
const STREAM_TOKEN_SCHEMA_DESCRIPTOR_V3: &str = concat!(
    "omnigraph.stream-token-authority.schema.v3\n",
    "id:utf8:required:unenforced-primary-key\n",
    "record_tag:utf8:required\n",
    "record_lookup_key:utf8:required\n",
    "stable_table_id:uint64:nullable\n",
    "table_incarnation_id:uint64:nullable\n",
    "logical_id:utf8:nullable\n",
    "origin_enrollment_id:utf8:nullable\n",
    "stream_incarnation_id:utf8:nullable\n",
    "current_token:utf8:nullable\n",
    "write_id:utf8:nullable\n",
    "predecessor_token:utf8:nullable\n",
    "disposition:utf8:nullable\n",
    "contributor_id:utf8:nullable\n",
    "payload_digest:utf8:nullable\n",
    "origin_kind:utf8:nullable\n",
    "origin_id:utf8:nullable\n",
    "origin_ordinal:uint64:nullable\n",
    "fold_base_token:utf8:nullable\n",
    "chain_depth:uint32:nullable\n",
    "terminal_correction_actor:utf8:nullable\n",
    "terminal_correction_operation_id:utf8:nullable\n",
    "terminal_dead_letter_json:utf8:nullable\n",
    "record_payload_json:utf8:nullable\n",
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
        Field::new("record_tag", DataType::Utf8, false),
        Field::new("record_lookup_key", DataType::Utf8, false),
        Field::new("stable_table_id", DataType::UInt64, true),
        Field::new("table_incarnation_id", DataType::UInt64, true),
        Field::new("logical_id", DataType::Utf8, true),
        Field::new("origin_enrollment_id", DataType::Utf8, true),
        Field::new("stream_incarnation_id", DataType::Utf8, true),
        Field::new("current_token", DataType::Utf8, true),
        Field::new("write_id", DataType::Utf8, true),
        Field::new("predecessor_token", DataType::Utf8, true),
        Field::new("disposition", DataType::Utf8, true),
        Field::new("contributor_id", DataType::Utf8, true),
        Field::new("payload_digest", DataType::Utf8, true),
        Field::new("origin_kind", DataType::Utf8, true),
        Field::new("origin_id", DataType::Utf8, true),
        Field::new("origin_ordinal", DataType::UInt64, true),
        Field::new("fold_base_token", DataType::Utf8, true),
        Field::new("chain_depth", DataType::UInt32, true),
        Field::new("terminal_correction_actor", DataType::Utf8, true),
        Field::new("terminal_correction_operation_id", DataType::Utf8, true),
        Field::new("terminal_dead_letter_json", DataType::Utf8, true),
        Field::new("record_payload_json", DataType::Utf8, true),
    ]))
}

pub(crate) fn stream_token_schema_hash() -> String {
    let digest = Sha256::digest(STREAM_TOKEN_SCHEMA_DESCRIPTOR_V3.as_bytes());
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

/// Encode complete current-token rows using the exact v3 physical schema.
pub(crate) fn stream_token_rows_to_batch(rows: &[StreamTokenAuthorityRow]) -> Result<RecordBatch> {
    if rows.is_empty() {
        return Err(OmniError::manifest_internal(
            "stream-token upsert requires at least one authority row",
        ));
    }

    let mut ids = Vec::with_capacity(rows.len());
    let mut record_tags = Vec::with_capacity(rows.len());
    let mut record_lookup_keys = Vec::with_capacity(rows.len());
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
    let mut terminal_dead_letters = Vec::with_capacity(rows.len());
    let mut record_payloads = Vec::with_capacity(rows.len());
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
        record_tags.push(CURRENT_TOKEN_RECORD_TAG);
        record_lookup_keys.push(stream_token_row_id(row.identity, &row.logical_id)?);
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
            StreamTokenDisposition::DeadLettered => "DEAD_LETTERED",
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
        terminal_dead_letters.push(
            row.terminal_dead_letter
                .as_deref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|error| {
                    OmniError::manifest_internal(format!(
                        "failed to encode terminal dead-letter evidence: {error}"
                    ))
                })?,
        );
        record_payloads.push(None::<String>);
    }

    RecordBatch::try_new(
        stream_token_schema(),
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(StringArray::from(record_tags)),
            Arc::new(StringArray::from(record_lookup_keys)),
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
            Arc::new(StringArray::from(terminal_dead_letters)),
            Arc::new(StringArray::from(record_payloads)),
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
pub(crate) fn validate_stream_token_plan_bounds(rows: &[StreamTokenAuthorityRow]) -> Result<()> {
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
            "stream-token scan returned a non-v3 physical schema",
        ));
    }
    let ids = required_string_array(batch, "id")?;
    let record_tags = required_string_array(batch, "record_tag")?;
    let record_lookup_keys = required_string_array(batch, "record_lookup_key")?;
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
    let terminal_dead_letters = required_string_array(batch, "terminal_dead_letter_json")?;
    let record_payloads = required_string_array(batch, "record_payload_json")?;

    let mut rows = Vec::with_capacity(batch.num_rows());
    let mut seen = std::collections::HashSet::with_capacity(batch.num_rows());
    for index in 0..batch.num_rows() {
        require_non_null(ids, index, "id")?;
        require_non_null(record_tags, index, "record_tag")?;
        require_non_null(record_lookup_keys, index, "record_lookup_key")?;
        if record_tags.value(index) != CURRENT_TOKEN_RECORD_TAG {
            return Err(OmniError::manifest_internal(format!(
                "current-token decoder received trusted record tag '{}'",
                record_tags.value(index)
            )));
        }
        if !record_payloads.is_null(index) {
            return Err(OmniError::manifest_internal(
                "current-token row must not carry a control-ledger payload",
            ));
        }
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
        if record_lookup_keys.value(index) != expected_id {
            return Err(OmniError::manifest_internal(format!(
                "stream-token row lookup key '{}' does not match canonical key '{}'",
                record_lookup_keys.value(index),
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
            "DEAD_LETTERED" => StreamTokenDisposition::DeadLettered,
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
        let terminal_dead_letter = if terminal_dead_letters.is_null(index) {
            None
        } else {
            let encoded = terminal_dead_letters.value(index);
            let evidence: StreamDeadLetterTerminalEvidence = serde_json::from_str(encoded)
                .map_err(|error| {
                    OmniError::manifest_internal(format!(
                        "failed to decode terminal dead-letter evidence: {error}"
                    ))
                })?;
            let canonical = serde_json::to_string(&evidence).map_err(|error| {
                OmniError::manifest_internal(format!(
                    "failed to re-encode terminal dead-letter evidence: {error}"
                ))
            })?;
            if canonical != encoded {
                return Err(OmniError::manifest_internal(
                    "terminal dead-letter evidence is not in canonical JSON field order",
                ));
            }
            Some(Box::new(evidence))
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
            terminal_dead_letter,
        };
        row.validate().map_err(stream_token_protocol_error)?;
        rows.push(row);
    }
    Ok(rows)
}

/// Stream every current-token row from one exact manifest-selected authority.
///
/// Retirement planning consumes this in bounded batches. The scan deliberately
/// has no ordering or history walk: its digests bind the selected authority
/// witness and aggregate counts, not an unbounded vector of terminal keys.
pub(crate) async fn scan_current_stream_token_batches(
    dataset: &Dataset,
    authority: &StreamTokenAuthorityEntry,
) -> Result<DatasetRecordBatchStream> {
    validate_exact_dataset(dataset, authority).await?;
    let mut scanner = dataset.scan();
    scanner.filter_expr(col("record_tag").eq(lit(CURRENT_TOKEN_RECORD_TAG)));
    scanner.batch_size(8_192);
    scanner.batch_size_bytes(crate::table_store::mem_wal::B2_MAX_TOKEN_PROJECTION_ARROW_BYTES);
    scanner
        .try_into_stream()
        .await
        .map_err(|error| OmniError::Lance(error.to_string()))
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
    scanner.filter_expr(
        col("record_tag")
            .eq(lit(CURRENT_TOKEN_RECORD_TAG))
            .and(col("record_lookup_key").eq(lit(id))),
    );
    scanner.batch_size(2);
    scanner.batch_size_bytes(crate::table_store::mem_wal::B2_MAX_TOKEN_PROJECTION_ARROW_BYTES);
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
            if retained_bytes > crate::table_store::mem_wal::B2_MAX_TOKEN_PROJECTION_ARROW_BYTES {
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
        || logical_ids.len() > crate::table_store::mem_wal::B1_MAX_GENERATION_ROWS as usize
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
    scanner.filter_expr(
        col("record_tag")
            .eq(lit(CURRENT_TOKEN_RECORD_TAG))
            .and(col("record_lookup_key").in_list(exact_ids.into_iter().map(lit).collect(), false)),
    );
    scanner.batch_size(logical_ids.len().saturating_add(1));
    scanner.batch_size_bytes(crate::table_store::mem_wal::B2_MAX_TOKEN_PROJECTION_ARROW_BYTES);
    scanner
        .limit(
            Some(
                i64::try_from(logical_ids.len().saturating_add(1)).map_err(|_| {
                    OmniError::manifest_internal("stream-token lookup row limit exceeds i64")
                })?,
            ),
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

pub(crate) fn lifecycle_ledger_records_to_batch(
    records: &[LifecycleLedgerRecord],
) -> Result<RecordBatch> {
    let envelopes = records
        .iter()
        .map(LifecycleLedgerRecord::to_envelope)
        .collect::<Result<Vec<_>>>()?;
    lifecycle_ledger_envelopes_to_batch(&envelopes)
}

pub(crate) fn lifecycle_ledger_records_from_batch(
    batch: &RecordBatch,
) -> Result<Vec<LifecycleLedgerRecord>> {
    lifecycle_ledger_envelopes_from_batch(batch, None)?
        .into_iter()
        .map(LifecycleLedgerRecord::from_envelope)
        .collect()
}

fn lifecycle_ledger_envelopes_to_batch(rows: &[LifecycleLedgerEnvelope]) -> Result<RecordBatch> {
    if rows.is_empty() {
        return Err(OmniError::manifest_internal(
            "lifecycle ledger staging requires at least one immutable record",
        ));
    }
    if rows.len() > MAX_LIFECYCLE_LEDGER_RECORDS_PER_TRANSACTION {
        return Err(OmniError::resource_limit(
            "stream_lifecycle_ledger_transaction_rows",
            MAX_LIFECYCLE_LEDGER_RECORDS_PER_TRANSACTION as u64,
            u64::try_from(rows.len()).unwrap_or(u64::MAX),
        ));
    }

    let mut seen_ids = std::collections::HashSet::with_capacity(rows.len());
    let mut seen_lookup_keys = std::collections::HashSet::with_capacity(rows.len());
    let mut total_json_bytes = 0_usize;
    for row in rows {
        for (name, value) in [
            ("record_id", row.record_id.as_str()),
            ("record_tag", row.record_tag.as_str()),
            ("record_lookup_key", row.record_lookup_key.as_str()),
        ] {
            if value.is_empty() || value.trim() != value {
                return Err(OmniError::manifest_internal(format!(
                    "lifecycle ledger {name} must be non-empty canonical text"
                )));
            }
        }
        if !seen_ids.insert(row.record_id.as_str()) {
            return Err(OmniError::manifest_internal(
                "lifecycle ledger transaction contains a duplicate immutable record id",
            ));
        }
        if !seen_lookup_keys.insert(row.record_lookup_key.as_str()) {
            return Err(OmniError::manifest_internal(
                "lifecycle ledger transaction contains a duplicate record lookup key",
            ));
        }
        let payload_bytes = row.record_payload_json.len();
        if payload_bytes > MAX_LIFECYCLE_LEDGER_RECORD_JSON_BYTES {
            return Err(OmniError::resource_limit(
                "stream_lifecycle_ledger_record_json_bytes",
                MAX_LIFECYCLE_LEDGER_RECORD_JSON_BYTES as u64,
                u64::try_from(payload_bytes).unwrap_or(u64::MAX),
            ));
        }
        total_json_bytes = total_json_bytes.checked_add(payload_bytes).ok_or_else(|| {
            OmniError::manifest_internal(
                "lifecycle ledger transaction JSON-byte accounting overflow",
            )
        })?;
        if total_json_bytes > MAX_LIFECYCLE_LEDGER_TRANSACTION_JSON_BYTES {
            return Err(OmniError::resource_limit(
                "stream_lifecycle_ledger_transaction_json_bytes",
                MAX_LIFECYCLE_LEDGER_TRANSACTION_JSON_BYTES as u64,
                u64::try_from(total_json_bytes).unwrap_or(u64::MAX),
            ));
        }
    }

    let ids = rows
        .iter()
        .map(|row| row.record_id.as_str())
        .collect::<Vec<_>>();
    let tags = rows
        .iter()
        .map(|row| row.record_tag.as_str())
        .collect::<Vec<_>>();
    let lookup_keys = rows
        .iter()
        .map(|row| row.record_lookup_key.as_str())
        .collect::<Vec<_>>();
    let payloads = rows
        .iter()
        .map(|row| row.record_payload_json.as_str())
        .collect::<Vec<_>>();
    let null_string =
        || Arc::new(StringArray::from(vec![None::<String>; rows.len()])) as Arc<dyn Array>;
    let batch = RecordBatch::try_new(
        stream_token_schema(),
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(StringArray::from(tags)),
            Arc::new(StringArray::from(lookup_keys)),
            Arc::new(UInt64Array::from(vec![None::<u64>; rows.len()])),
            Arc::new(UInt64Array::from(vec![None::<u64>; rows.len()])),
            null_string(),
            null_string(),
            null_string(),
            null_string(),
            null_string(),
            null_string(),
            null_string(),
            null_string(),
            null_string(),
            null_string(),
            null_string(),
            Arc::new(UInt64Array::from(vec![None::<u64>; rows.len()])),
            null_string(),
            Arc::new(UInt32Array::from(vec![None::<u32>; rows.len()])),
            null_string(),
            null_string(),
            null_string(),
            Arc::new(StringArray::from(payloads)),
        ],
    )
    .map_err(|error| {
        OmniError::manifest_internal(format!("failed to build lifecycle ledger batch: {error}"))
    })?;
    let arrow_bytes = u64::try_from(batch.get_array_memory_size())
        .map_err(|_| OmniError::manifest_internal("lifecycle ledger Arrow size exceeds u64"))?;
    if arrow_bytes > MAX_LIFECYCLE_LEDGER_TRANSACTION_ARROW_BYTES {
        return Err(OmniError::resource_limit(
            "stream_lifecycle_ledger_transaction_arrow_bytes",
            MAX_LIFECYCLE_LEDGER_TRANSACTION_ARROW_BYTES,
            arrow_bytes,
        ));
    }
    Ok(batch)
}

fn lifecycle_ledger_envelopes_from_batch(
    batch: &RecordBatch,
    expected_tag: Option<&str>,
) -> Result<Vec<LifecycleLedgerEnvelope>> {
    if batch.schema().as_ref() != stream_token_schema().as_ref() {
        return Err(OmniError::manifest_internal(
            "lifecycle ledger scan returned a non-v3 physical schema",
        ));
    }
    if batch.num_rows() > MAX_LIFECYCLE_LEDGER_RECORDS_PER_TRANSACTION {
        return Err(OmniError::resource_limit(
            "stream_lifecycle_ledger_transaction_rows",
            MAX_LIFECYCLE_LEDGER_RECORDS_PER_TRANSACTION as u64,
            u64::try_from(batch.num_rows()).unwrap_or(u64::MAX),
        ));
    }
    let arrow_bytes = u64::try_from(batch.get_array_memory_size())
        .map_err(|_| OmniError::manifest_internal("lifecycle ledger Arrow size exceeds u64"))?;
    if arrow_bytes > MAX_LIFECYCLE_LEDGER_TRANSACTION_ARROW_BYTES {
        return Err(OmniError::resource_limit(
            "stream_lifecycle_ledger_transaction_arrow_bytes",
            MAX_LIFECYCLE_LEDGER_TRANSACTION_ARROW_BYTES,
            arrow_bytes,
        ));
    }

    let ids = required_string_array(batch, "id")?;
    let tags = required_string_array(batch, "record_tag")?;
    let lookup_keys = required_string_array(batch, "record_lookup_key")?;
    let payloads = required_string_array(batch, "record_payload_json")?;
    let control_null_columns = [
        "stable_table_id",
        "table_incarnation_id",
        "logical_id",
        "origin_enrollment_id",
        "stream_incarnation_id",
        "current_token",
        "write_id",
        "predecessor_token",
        "disposition",
        "contributor_id",
        "payload_digest",
        "origin_kind",
        "origin_id",
        "origin_ordinal",
        "fold_base_token",
        "chain_depth",
        "terminal_correction_actor",
        "terminal_correction_operation_id",
        "terminal_dead_letter_json",
    ];
    let mut rows = Vec::with_capacity(batch.num_rows());
    let mut seen_ids = std::collections::HashSet::with_capacity(batch.num_rows());
    let mut seen_lookup_keys = std::collections::HashSet::with_capacity(batch.num_rows());
    let mut total_json_bytes = 0_usize;
    for index in 0..batch.num_rows() {
        require_non_null(ids, index, "id")?;
        require_non_null(tags, index, "record_tag")?;
        require_non_null(lookup_keys, index, "record_lookup_key")?;
        require_non_null(payloads, index, "record_payload_json")?;
        if expected_tag.is_some_and(|expected| tags.value(index) != expected) {
            return Err(OmniError::manifest_internal(format!(
                "lifecycle ledger decoder expected trusted record tag '{}' but received '{}'",
                expected_tag.expect("checked above"),
                tags.value(index)
            )));
        }
        for name in control_null_columns {
            let column = batch.column_by_name(name).ok_or_else(|| {
                OmniError::manifest_internal(format!("lifecycle ledger batch is missing '{name}'"))
            })?;
            if !column.is_null(index) {
                return Err(OmniError::manifest_internal(format!(
                    "lifecycle ledger row has non-null current-token column '{name}'"
                )));
            }
        }
        let payload_bytes = payloads.value(index).len();
        if payload_bytes > MAX_LIFECYCLE_LEDGER_RECORD_JSON_BYTES {
            return Err(OmniError::resource_limit(
                "stream_lifecycle_ledger_record_json_bytes",
                MAX_LIFECYCLE_LEDGER_RECORD_JSON_BYTES as u64,
                u64::try_from(payload_bytes).unwrap_or(u64::MAX),
            ));
        }
        total_json_bytes = total_json_bytes.checked_add(payload_bytes).ok_or_else(|| {
            OmniError::manifest_internal(
                "lifecycle ledger transaction JSON-byte accounting overflow",
            )
        })?;
        if total_json_bytes > MAX_LIFECYCLE_LEDGER_TRANSACTION_JSON_BYTES {
            return Err(OmniError::resource_limit(
                "stream_lifecycle_ledger_transaction_json_bytes",
                MAX_LIFECYCLE_LEDGER_TRANSACTION_JSON_BYTES as u64,
                u64::try_from(total_json_bytes).unwrap_or(u64::MAX),
            ));
        }
        if !seen_ids.insert(ids.value(index)) {
            return Err(OmniError::manifest_internal(
                "lifecycle ledger batch contains a duplicate immutable record id",
            ));
        }
        if !seen_lookup_keys.insert(lookup_keys.value(index)) {
            return Err(OmniError::manifest_internal(
                "lifecycle ledger batch contains a duplicate record lookup key",
            ));
        }
        rows.push(LifecycleLedgerEnvelope {
            record_id: ids.value(index).to_string(),
            record_tag: tags.value(index).to_string(),
            record_lookup_key: lookup_keys.value(index).to_string(),
            record_payload_json: payloads.value(index).to_string(),
        });
    }
    Ok(rows)
}

async fn lookup_lifecycle_ledger_envelope(
    dataset: &Dataset,
    authority: &StreamTokenAuthorityEntry,
    expected_tag: &str,
    record_lookup_key: &str,
) -> Result<Option<LifecycleLedgerEnvelope>> {
    validate_exact_dataset(dataset, authority).await?;
    for (name, value) in [
        ("record tag", expected_tag),
        ("record lookup key", record_lookup_key),
    ] {
        if value.is_empty() || value.trim() != value {
            return Err(OmniError::manifest_internal(format!(
                "lifecycle ledger {name} must be non-empty canonical text"
            )));
        }
    }
    let mut scanner = dataset.scan();
    scanner.filter_expr(
        col("record_tag")
            .eq(lit(expected_tag))
            .and(col("record_lookup_key").eq(lit(record_lookup_key))),
    );
    scanner.batch_size(2);
    scanner.batch_size_bytes(MAX_LIFECYCLE_LEDGER_TRANSACTION_ARROW_BYTES);
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
        for row in lifecycle_ledger_envelopes_from_batch(&batch, Some(expected_tag))? {
            if selected.replace(row).is_some() {
                return Err(OmniError::manifest_internal(format!(
                    "stream-token ledger contains duplicate {expected_tag} operation rows"
                )));
            }
        }
    }
    if selected
        .as_ref()
        .is_some_and(|row| row.record_lookup_key != record_lookup_key)
    {
        return Err(OmniError::manifest_internal(
            "lifecycle ledger exact lookup returned a row for another lookup key",
        ));
    }
    Ok(selected)
}

pub(crate) async fn lookup_lifecycle_ledger_record(
    dataset: &Dataset,
    authority: &StreamTokenAuthorityEntry,
    expected_tag: &str,
    record_lookup_key: &str,
) -> Result<Option<LifecycleLedgerRecord>> {
    lookup_lifecycle_ledger_envelope(dataset, authority, expected_tag, record_lookup_key)
        .await?
        .map(LifecycleLedgerRecord::from_envelope)
        .transpose()
}

/// Resolve one manifest-selected immutable ledger head by its exact record ID.
///
/// Lifecycle rows retain the selected record ID rather than the operation
/// lookup key.  This probe is deliberately capped at two rows so a duplicate
/// unenforced primary key is diagnosed instead of silently choosing one.
pub(crate) async fn lookup_lifecycle_ledger_record_by_id(
    dataset: &Dataset,
    authority: &StreamTokenAuthorityEntry,
    expected_tag: &str,
    record_id: &str,
) -> Result<Option<LifecycleLedgerRecord>> {
    validate_exact_dataset(dataset, authority).await?;
    for (name, value) in [("record tag", expected_tag), ("record id", record_id)] {
        if value.is_empty() || value.trim() != value {
            return Err(OmniError::manifest_internal(format!(
                "lifecycle ledger {name} must be non-empty canonical text"
            )));
        }
    }
    let mut scanner = dataset.scan();
    scanner.filter_expr(
        col("record_tag")
            .eq(lit(expected_tag))
            .and(col("id").eq(lit(record_id))),
    );
    scanner.batch_size(2);
    scanner.batch_size_bytes(MAX_LIFECYCLE_LEDGER_TRANSACTION_ARROW_BYTES);
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
        for row in lifecycle_ledger_envelopes_from_batch(&batch, Some(expected_tag))? {
            if selected.replace(row).is_some() {
                return Err(OmniError::manifest_internal(format!(
                    "stream-token ledger contains duplicate immutable record id '{record_id}'"
                )));
            }
        }
    }
    if selected
        .as_ref()
        .is_some_and(|row| row.record_id != record_id)
    {
        return Err(OmniError::manifest_internal(
            "lifecycle ledger exact lookup returned a row for another record id",
        ));
    }
    selected
        .map(LifecycleLedgerRecord::from_envelope)
        .transpose()
}

pub(crate) async fn lookup_enrollment_receipt_v2(
    dataset: &Dataset,
    authority: &StreamTokenAuthorityEntry,
    graph_identity_digest: &str,
    identity: TableIdentity,
    enrollment_request_id: &str,
) -> Result<Option<EnrollmentReceiptV2>> {
    let lookup_key = EnrollmentReceiptV2::lookup_key_for(
        graph_identity_digest,
        identity,
        enrollment_request_id,
    )?;
    match lookup_lifecycle_ledger_record(dataset, authority, ENROLLMENT_RECEIPT_V2_TAG, &lookup_key)
        .await?
    {
        Some(LifecycleLedgerRecord::EnrollmentReceiptV2(value)) => Ok(Some(value)),
        None => Ok(None),
        Some(_) => Err(OmniError::manifest_internal(
            "enrollment receipt lookup decoded another lifecycle ledger family",
        )),
    }
}

pub(crate) async fn lookup_binding_receipt(
    dataset: &Dataset,
    authority: &StreamTokenAuthorityEntry,
    graph_identity_digest: &str,
    identity: TableIdentity,
    binding_scope_id: &str,
    operation_id: &str,
) -> Result<Option<BindingReceipt>> {
    let lookup_key = BindingReceipt::lookup_key_for(
        graph_identity_digest,
        identity,
        binding_scope_id,
        operation_id,
    )?;
    match lookup_lifecycle_ledger_record(dataset, authority, BINDING_RECEIPT_TAG, &lookup_key)
        .await?
    {
        Some(LifecycleLedgerRecord::BindingReceipt(value)) => Ok(Some(value)),
        None => Ok(None),
        Some(_) => Err(OmniError::manifest_internal(
            "binding receipt lookup decoded another lifecycle ledger family",
        )),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn lookup_management_receipt(
    dataset: &Dataset,
    authority: &StreamTokenAuthorityEntry,
    graph_identity_digest: &str,
    identity: TableIdentity,
    stream_incarnation_id: &str,
    operation_kind: &str,
    operation_id: &str,
) -> Result<Option<ManagementReceipt>> {
    let lookup_key = ManagementReceipt::lookup_key_for(
        graph_identity_digest,
        identity,
        stream_incarnation_id,
        operation_kind,
        operation_id,
    )?;
    match lookup_lifecycle_ledger_record(dataset, authority, MANAGEMENT_RECEIPT_TAG, &lookup_key)
        .await?
    {
        Some(LifecycleLedgerRecord::ManagementReceipt(value)) => Ok(Some(value)),
        None => Ok(None),
        Some(_) => Err(OmniError::manifest_internal(
            "management receipt lookup decoded another lifecycle ledger family",
        )),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn lookup_stream_correction_receipt(
    dataset: &Dataset,
    authority: &StreamTokenAuthorityEntry,
    graph_identity_digest: &str,
    identity: TableIdentity,
    stream_incarnation_id: &str,
    block_token: &str,
    correction_id: &str,
) -> Result<Option<StreamCorrectionReceipt>> {
    let lookup_key = StreamCorrectionReceipt::lookup_key_for(
        graph_identity_digest,
        identity,
        stream_incarnation_id,
        block_token,
        correction_id,
    )?;
    match lookup_lifecycle_ledger_record(
        dataset,
        authority,
        STREAM_CORRECTION_RECEIPT_TAG,
        &lookup_key,
    )
    .await?
    {
        Some(LifecycleLedgerRecord::StreamCorrectionReceipt(value)) => Ok(Some(value)),
        None => Ok(None),
        Some(_) => Err(OmniError::manifest_internal(
            "correction receipt lookup decoded another lifecycle ledger family",
        )),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn lookup_claim_attempt_effect(
    dataset: &Dataset,
    authority: &StreamTokenAuthorityEntry,
    graph_identity_digest: &str,
    identity: TableIdentity,
    binding_scope_id: &str,
    claim_id: &str,
    ordinal: u64,
) -> Result<Option<ClaimAttemptEffect>> {
    let lookup_key = ClaimAttemptEffect::lookup_key_for(
        graph_identity_digest,
        identity,
        binding_scope_id,
        claim_id,
        ordinal,
    )?;
    match lookup_lifecycle_ledger_record(dataset, authority, CLAIM_ATTEMPT_EFFECT_TAG, &lookup_key)
        .await?
    {
        Some(LifecycleLedgerRecord::ClaimAttemptEffect(value)) => Ok(Some(value)),
        None => Ok(None),
        Some(_) => Err(OmniError::manifest_internal(
            "claim-attempt lookup decoded another lifecycle ledger family",
        )),
    }
}

pub(crate) async fn lookup_claim_receipt(
    dataset: &Dataset,
    authority: &StreamTokenAuthorityEntry,
    graph_identity_digest: &str,
    identity: TableIdentity,
    binding_scope_id: &str,
    claim_id: &str,
) -> Result<Option<ClaimReceipt>> {
    let lookup_key =
        ClaimReceipt::lookup_key_for(graph_identity_digest, identity, binding_scope_id, claim_id)?;
    match lookup_lifecycle_ledger_record(dataset, authority, CLAIM_RECEIPT_TAG, &lookup_key).await?
    {
        Some(LifecycleLedgerRecord::ClaimReceipt(value)) => Ok(Some(value)),
        None => Ok(None),
        Some(_) => Err(OmniError::manifest_internal(
            "claim receipt lookup decoded another lifecycle ledger family",
        )),
    }
}

/// Receipt-first lookup for the root-wide retirement occurrence. This must run
/// before comparing the current profile so a retry after the terminal CAS can
/// return its immutable result.
pub(crate) async fn lookup_authority_retirement_receipt(
    dataset: &Dataset,
    authority: &StreamTokenAuthorityEntry,
    graph_identity_digest: &str,
    retirement_id: &str,
) -> Result<Option<AuthorityRetirementReceipt>> {
    let lookup_key =
        AuthorityRetirementReceipt::lookup_key_for(graph_identity_digest, retirement_id)?;
    match lookup_lifecycle_ledger_record(
        dataset,
        authority,
        AUTHORITY_RETIREMENT_RECEIPT_TAG,
        &lookup_key,
    )
    .await?
    {
        Some(LifecycleLedgerRecord::AuthorityRetirementReceipt(value)) => Ok(Some(value)),
        None => Ok(None),
        Some(_) => Err(OmniError::manifest_internal(
            "authority-retirement lookup decoded another lifecycle ledger family",
        )),
    }
}

/// Receipt-first lookup for the F5 three-disposition retirement occurrence.
pub(crate) async fn lookup_authority_retirement_receipt_v2(
    dataset: &Dataset,
    authority: &StreamTokenAuthorityEntry,
    graph_identity_digest: &str,
    retirement_id: &str,
) -> Result<Option<AuthorityRetirementReceiptV2>> {
    let lookup_key =
        AuthorityRetirementReceiptV2::lookup_key_for(graph_identity_digest, retirement_id)
            .map_err(stream_token_protocol_error)?;
    match lookup_lifecycle_ledger_record(
        dataset,
        authority,
        AUTHORITY_RETIREMENT_RECEIPT_V2_TAG,
        &lookup_key,
    )
    .await?
    {
        Some(LifecycleLedgerRecord::AuthorityRetirementReceiptV2(value)) => Ok(Some(value)),
        None => Ok(None),
        Some(_) => Err(OmniError::manifest_internal(
            "authority-retirement-v2 lookup decoded another lifecycle ledger family",
        )),
    }
}

async fn stage_lifecycle_ledger_envelopes(
    dataset: Dataset,
    authority: &StreamTokenAuthorityEntry,
    rows: &[LifecycleLedgerEnvelope],
) -> Result<crate::table_store::StagedWrite> {
    validate_exact_dataset(&dataset, authority).await?;
    let batch = lifecycle_ledger_envelopes_to_batch(rows)?;
    let row_count = u64::try_from(batch.num_rows())
        .map_err(|_| OmniError::manifest_internal("lifecycle ledger row count exceeds u64"))?;
    let schema = batch.schema();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let stream = lance_datafusion::utils::reader_to_stream(Box::new(reader));
    let mut builder =
        MergeInsertBuilder::try_new(Arc::new(dataset.clone()), vec!["id".to_string()])
            .map_err(|error| OmniError::Lance(error.to_string()))?;
    builder
        .when_matched(WhenMatched::Fail)
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
            "lifecycle ledger staged transaction read version {} does not match manifest-selected version {}",
            uncommitted.transaction.read_version, authority.current_head_witness.table_version
        )));
    }
    crate::table_store::staged_exact_id_upsert_result(
        &dataset,
        uncommitted,
        row_count,
        "stage_lifecycle_ledger_records",
    )
}

pub(crate) async fn stage_lifecycle_ledger_records(
    dataset: Dataset,
    authority: &StreamTokenAuthorityEntry,
    records: &[LifecycleLedgerRecord],
) -> Result<crate::table_store::StagedWrite> {
    let envelopes = records
        .iter()
        .map(LifecycleLedgerRecord::to_envelope)
        .collect::<Result<Vec<_>>>()?;
    stage_lifecycle_ledger_envelopes(dataset, authority, &envelopes).await
}

pub(crate) async fn stage_enrollment_receipt_v2(
    dataset: Dataset,
    authority: &StreamTokenAuthorityEntry,
    receipt: &EnrollmentReceiptV2,
) -> Result<crate::table_store::StagedWrite> {
    stage_lifecycle_ledger_records(
        dataset,
        authority,
        &[LifecycleLedgerRecord::EnrollmentReceiptV2(receipt.clone())],
    )
    .await
}

pub(crate) async fn stage_binding_receipt(
    dataset: Dataset,
    authority: &StreamTokenAuthorityEntry,
    receipt: &BindingReceipt,
) -> Result<crate::table_store::StagedWrite> {
    stage_lifecycle_ledger_records(
        dataset,
        authority,
        &[LifecycleLedgerRecord::BindingReceipt(receipt.clone())],
    )
    .await
}

pub(crate) async fn stage_management_receipt(
    dataset: Dataset,
    authority: &StreamTokenAuthorityEntry,
    receipt: &ManagementReceipt,
) -> Result<crate::table_store::StagedWrite> {
    stage_lifecycle_ledger_records(
        dataset,
        authority,
        &[LifecycleLedgerRecord::ManagementReceipt(receipt.clone())],
    )
    .await
}

pub(crate) async fn stage_claim_attempt_effect(
    dataset: Dataset,
    authority: &StreamTokenAuthorityEntry,
    receipt: &ClaimAttemptEffect,
) -> Result<crate::table_store::StagedWrite> {
    stage_lifecycle_ledger_records(
        dataset,
        authority,
        &[LifecycleLedgerRecord::ClaimAttemptEffect(receipt.clone())],
    )
    .await
}

pub(crate) async fn stage_claim_receipt(
    dataset: Dataset,
    authority: &StreamTokenAuthorityEntry,
    receipt: &ClaimReceipt,
) -> Result<crate::table_store::StagedWrite> {
    stage_lifecycle_ledger_records(
        dataset,
        authority,
        &[LifecycleLedgerRecord::ClaimReceipt(receipt.clone())],
    )
    .await
}

pub(crate) async fn stage_authority_retirement_receipt(
    dataset: Dataset,
    authority: &StreamTokenAuthorityEntry,
    receipt: &AuthorityRetirementReceipt,
) -> Result<crate::table_store::StagedWrite> {
    stage_lifecycle_ledger_records(
        dataset,
        authority,
        &[LifecycleLedgerRecord::AuthorityRetirementReceipt(
            receipt.clone(),
        )],
    )
    .await
}

pub(crate) async fn stage_authority_retirement_receipt_v2(
    dataset: Dataset,
    authority: &StreamTokenAuthorityEntry,
    receipt: &AuthorityRetirementReceiptV2,
) -> Result<crate::table_store::StagedWrite> {
    stage_lifecycle_ledger_records(
        dataset,
        authority,
        &[LifecycleLedgerRecord::AuthorityRetirementReceiptV2(
            receipt.clone(),
        )],
    )
    .await
}

const MAX_PROFILE_MANAGEMENT_RECEIPT_JSON_BYTES: usize = 64 * 1024;

/// Encode one immutable profile-management ledger row using the tagged v3
/// union schema. Every current-token column is structurally null.
pub(crate) fn profile_management_receipt_to_batch(
    receipt: &ProfileManagementReceipt,
) -> Result<RecordBatch> {
    receipt.validate()?;
    let payload = serde_json::to_string(receipt).map_err(|error| {
        OmniError::manifest_internal(format!(
            "failed to encode profile-management receipt: {error}"
        ))
    })?;
    if payload.len() > MAX_PROFILE_MANAGEMENT_RECEIPT_JSON_BYTES {
        return Err(OmniError::resource_limit(
            "stream_profile_receipt_json_bytes",
            MAX_PROFILE_MANAGEMENT_RECEIPT_JSON_BYTES as u64,
            payload.len() as u64,
        ));
    }
    let null_string = || Arc::new(StringArray::from(vec![None::<String>]));
    RecordBatch::try_new(
        stream_token_schema(),
        vec![
            Arc::new(StringArray::from(vec![receipt.record_id.clone()])),
            Arc::new(StringArray::from(vec![PROFILE_MANAGEMENT_RECEIPT_TAG])),
            Arc::new(StringArray::from(vec![receipt.record_lookup_key.clone()])),
            Arc::new(UInt64Array::from(vec![None::<u64>])),
            Arc::new(UInt64Array::from(vec![None::<u64>])),
            null_string(),
            null_string(),
            null_string(),
            null_string(),
            null_string(),
            null_string(),
            null_string(),
            null_string(),
            null_string(),
            null_string(),
            null_string(),
            Arc::new(UInt64Array::from(vec![None::<u64>])),
            null_string(),
            Arc::new(UInt32Array::from(vec![None::<u32>])),
            null_string(),
            null_string(),
            null_string(),
            Arc::new(StringArray::from(vec![Some(payload)])),
        ],
    )
    .map_err(|error| {
        OmniError::manifest_internal(format!(
            "failed to build profile-management receipt batch: {error}"
        ))
    })
}

/// Decode tagged profile-management rows. Passing a current-token row or any
/// mixed-population row is a hard protocol error.
pub(crate) fn profile_management_receipts_from_batch(
    batch: &RecordBatch,
) -> Result<Vec<ProfileManagementReceipt>> {
    if batch.schema().as_ref() != stream_token_schema().as_ref() {
        return Err(OmniError::manifest_internal(
            "profile-management receipt scan returned a non-v3 physical schema",
        ));
    }
    let ids = required_string_array(batch, "id")?;
    let tags = required_string_array(batch, "record_tag")?;
    let lookup_keys = required_string_array(batch, "record_lookup_key")?;
    let payloads = required_string_array(batch, "record_payload_json")?;
    let token_columns = [
        "stable_table_id",
        "table_incarnation_id",
        "logical_id",
        "origin_enrollment_id",
        "stream_incarnation_id",
        "current_token",
        "write_id",
        "predecessor_token",
        "disposition",
        "contributor_id",
        "payload_digest",
        "origin_kind",
        "origin_id",
        "origin_ordinal",
        "fold_base_token",
        "chain_depth",
        "terminal_correction_actor",
        "terminal_correction_operation_id",
        "terminal_dead_letter_json",
    ];
    let mut receipts = Vec::with_capacity(batch.num_rows());
    for index in 0..batch.num_rows() {
        require_non_null(ids, index, "id")?;
        require_non_null(tags, index, "record_tag")?;
        require_non_null(lookup_keys, index, "record_lookup_key")?;
        require_non_null(payloads, index, "record_payload_json")?;
        if tags.value(index) != PROFILE_MANAGEMENT_RECEIPT_TAG {
            return Err(OmniError::manifest_internal(format!(
                "profile-management decoder received trusted record tag '{}'",
                tags.value(index)
            )));
        }
        for name in token_columns {
            let column = batch.column_by_name(name).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "profile-management receipt batch is missing '{name}'"
                ))
            })?;
            if !column.is_null(index) {
                return Err(OmniError::manifest_internal(format!(
                    "profile-management receipt has non-null current-token column '{name}'"
                )));
            }
        }
        let payload = payloads.value(index);
        if payload.len() > MAX_PROFILE_MANAGEMENT_RECEIPT_JSON_BYTES {
            return Err(OmniError::resource_limit(
                "stream_profile_receipt_json_bytes",
                MAX_PROFILE_MANAGEMENT_RECEIPT_JSON_BYTES as u64,
                payload.len() as u64,
            ));
        }
        let receipt: ProfileManagementReceipt = serde_json::from_str(payload).map_err(|error| {
            OmniError::manifest_internal(format!(
                "failed to decode profile-management receipt: {error}"
            ))
        })?;
        receipt.validate()?;
        if ids.value(index) != receipt.record_id
            || lookup_keys.value(index) != receipt.record_lookup_key
            || tags.value(index) != receipt.record_tag
        {
            return Err(OmniError::manifest_internal(
                "profile-management receipt physical envelope differs from its canonical payload",
            ));
        }
        receipts.push(receipt);
    }
    Ok(receipts)
}

/// Receipt-first operation lookup. Callers invoke this before comparing the
/// current profile revision so a delayed exact retry returns its original
/// bounded result rather than targeting a later profile cycle.
pub(crate) async fn lookup_profile_management_receipt(
    dataset: &Dataset,
    authority: &StreamTokenAuthorityEntry,
    graph_identity_digest: &str,
    operation_id: &str,
) -> Result<Option<ProfileManagementReceipt>> {
    validate_exact_dataset(dataset, authority).await?;
    let lookup_key = ProfileManagementReceipt::lookup_key_for(graph_identity_digest, operation_id)?;
    let mut scanner = dataset.scan();
    scanner.filter_expr(
        col("record_tag")
            .eq(lit(PROFILE_MANAGEMENT_RECEIPT_TAG))
            .and(col("record_lookup_key").eq(lit(lookup_key))),
    );
    scanner.batch_size(2);
    scanner.batch_size_bytes(MAX_PROFILE_MANAGEMENT_RECEIPT_JSON_BYTES as u64);
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
        for receipt in profile_management_receipts_from_batch(&batch)? {
            if selected.replace(receipt).is_some() {
                return Err(OmniError::manifest_internal(
                    "stream-token ledger contains duplicate profile-management operation rows",
                ));
            }
        }
    }
    if selected.as_ref().is_some_and(|receipt| {
        receipt.graph_identity_digest != graph_identity_digest
            || receipt.operation_id != operation_id
    }) {
        return Err(OmniError::manifest_internal(
            "profile-management lookup returned a row for another operation scope",
        ));
    }
    Ok(selected)
}

/// Stage one immutable receipt insertion without advancing Lance HEAD.
///
/// `WhenMatched::Fail` is load-bearing: an operation id can never be rebound to
/// another request/result. Exact retries perform [`lookup_profile_management_receipt`]
/// before staging.
pub(crate) async fn stage_profile_management_receipt(
    dataset: Dataset,
    authority: &StreamTokenAuthorityEntry,
    receipt: &ProfileManagementReceipt,
) -> Result<crate::table_store::StagedWrite> {
    validate_exact_dataset(&dataset, authority).await?;
    let batch = profile_management_receipt_to_batch(receipt)?;
    let schema = batch.schema();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let stream = lance_datafusion::utils::reader_to_stream(Box::new(reader));
    let mut builder =
        MergeInsertBuilder::try_new(Arc::new(dataset.clone()), vec!["id".to_string()])
            .map_err(|error| OmniError::Lance(error.to_string()))?;
    builder
        .when_matched(WhenMatched::Fail)
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
            "profile-management staged transaction read version {} does not match manifest-selected version {}",
            uncommitted.transaction.read_version, authority.current_head_witness.table_version
        )));
    }
    crate::table_store::staged_exact_id_upsert_result(
        &dataset,
        uncommitted,
        1,
        "stage_profile_management_receipt",
    )
}

/// Stage one exact-`id` current-token upsert without advancing Lance HEAD.
///
/// `dataset` must be the exact handle selected by `authority`; the helper
/// validates that witness before producing any staged files. The returned
/// [`crate::table_store::StagedWrite`] must enter recovery-v12 before its one
/// strict `commit_staged_exact` invocation.
pub(crate) async fn stage_stream_token_and_lifecycle_records(
    dataset: Dataset,
    authority: &StreamTokenAuthorityEntry,
    token_rows: &[StreamTokenAuthorityRow],
    records: &[LifecycleLedgerRecord],
) -> Result<crate::table_store::StagedWrite> {
    validate_exact_dataset(&dataset, authority).await?;
    if records.is_empty() {
        return Err(OmniError::manifest_internal(
            "mixed stream-token staging requires at least one immutable lifecycle record",
        ));
    }
    if !token_rows.is_empty() {
        validate_stream_token_plan_bounds(token_rows)?;
    }
    for record in records {
        record.validate()?;
        // The mixed transaction uses UpdateAll for current-token rows. Prove
        // every immutable operation lookup key and record id absent in the
        // selected version first so that policy can never reinterpret an
        // operation id or rewrite a receipt. A later competing commit still
        // loses the exact read-version commit; transparent retries are off.
        if lookup_lifecycle_ledger_envelope(
            &dataset,
            authority,
            record.record_tag(),
            record.record_lookup_key(),
        )
        .await?
        .is_some()
        {
            return Err(OmniError::manifest_conflict(format!(
                "immutable lifecycle record id or operation lookup key '{}' already exists at the manifest-selected stream-token version",
                record.record_lookup_key()
            )));
        }
        let mut scanner = dataset.scan();
        scanner.filter_expr(col("id").eq(lit(record.record_id())));
        scanner.batch_size(2);
        scanner.batch_size_bytes(MAX_LIFECYCLE_LEDGER_TRANSACTION_ARROW_BYTES);
        scanner
            .limit(Some(2), None)
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        let mut stream = scanner
            .try_into_stream()
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        let mut selected_rows = 0usize;
        while let Some(batch) = stream
            .try_next()
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?
        {
            selected_rows = selected_rows.checked_add(batch.num_rows()).ok_or_else(|| {
                OmniError::manifest_internal("immutable record preflight row-count overflow")
            })?;
        }
        if selected_rows != 0 {
            return Err(OmniError::manifest_conflict(format!(
                "immutable lifecycle record id '{}' already exists at the manifest-selected stream-token version",
                record.record_id()
            )));
        }
    }

    let ledger_batch = lifecycle_ledger_records_to_batch(records)?;
    let batch = if token_rows.is_empty() {
        ledger_batch
    } else {
        let token_batch = stream_token_rows_to_batch(token_rows)?;
        let schema = token_batch.schema();
        arrow_select::concat::concat_batches(&schema, &[token_batch, ledger_batch]).map_err(
            |error| {
                OmniError::manifest_internal(format!(
                    "failed to combine current-token and lifecycle-ledger rows: {error}"
                ))
            },
        )?
    };
    let combined_limit = crate::table_store::mem_wal::B2_MAX_TOKEN_PROJECTION_ARROW_BYTES
        .checked_add(MAX_LIFECYCLE_LEDGER_TRANSACTION_ARROW_BYTES)
        .ok_or_else(|| OmniError::manifest_internal("mixed staging byte limit overflow"))?;
    let batch_bytes = u64::try_from(batch.get_array_memory_size()).map_err(|_| {
        OmniError::manifest_internal("mixed stream-token staging Arrow size exceeds u64")
    })?;
    if batch_bytes > combined_limit {
        return Err(OmniError::resource_limit(
            "stream_token_and_lifecycle_arrow_bytes",
            combined_limit,
            batch_bytes,
        ));
    }
    let row_count = u64::try_from(batch.num_rows())
        .map_err(|_| OmniError::manifest_internal("mixed staging row count exceeds u64"))?;
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
            "mixed stream-token transaction read version {} does not match manifest-selected version {}",
            uncommitted.transaction.read_version, authority.current_head_witness.table_version
        )));
    }
    crate::table_store::staged_exact_id_upsert_result(
        &dataset,
        uncommitted,
        row_count,
        "stage_stream_token_and_lifecycle_records",
    )
}

pub(crate) async fn stage_stream_correction_effect(
    dataset: Dataset,
    authority: &StreamTokenAuthorityEntry,
    token_rows: &[StreamTokenAuthorityRow],
    correction_receipt: &StreamCorrectionReceipt,
    management_receipt: &ManagementReceipt,
) -> Result<crate::table_store::StagedWrite> {
    correction_receipt.validate()?;
    management_receipt.validate(management_receipt.to_revision)?;
    if management_receipt.operation_kind != super::stream::STREAM_CORRECTION_OPERATION_KIND
        || management_receipt.operation_id != correction_receipt.correction_id
        || management_receipt.graph_identity_digest != correction_receipt.graph_identity_digest
        || management_receipt.identity != correction_receipt.identity
        || management_receipt.stream_incarnation_id != correction_receipt.stream_incarnation_id
        || management_receipt.binding_scope_id != correction_receipt.binding_scope_id
        || management_receipt.actor_id != correction_receipt.actor_id
        || management_receipt.to_revision != correction_receipt.resulting_lifecycle_revision
        || management_receipt.result_payload != correction_receipt.result_payload
        || management_receipt.result_digest != correction_receipt.result_digest
        || management_receipt.recorded_at != correction_receipt.recorded_at
    {
        return Err(OmniError::manifest_internal(
            "stream correction and management receipts do not describe one exact terminal result",
        ));
    }
    stage_stream_token_and_lifecycle_records(
        dataset,
        authority,
        token_rows,
        &[
            LifecycleLedgerRecord::StreamCorrectionReceipt(correction_receipt.clone()),
            LifecycleLedgerRecord::ManagementReceipt(management_receipt.clone()),
        ],
    )
    .await
}

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
    let mut dataset = Dataset::write(reader, &stream_token_uri(root_uri), Some(params))
        .await
        .map_err(|error| OmniError::Lance(error.to_string()))?;
    dataset
        .create_index(
            &["record_lookup_key"],
            IndexType::BTree,
            Some("stream_control_record_lookup_v1".to_string()),
            &ScalarIndexParams::default(),
            true,
        )
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
            "cannot publish a stream-token dataset with a non-v2 schema",
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
            "manifest-selected stream-token dataset has a schema different from its v2 authority",
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
    use crate::db::manifest::stream::{
        ClaimAttemptClassification, ClaimAttemptEffectPreimage, ClaimProfile, ClaimReceiptPreimage,
        ClaimTerminalClassification, StreamCorrectionReceiptPreimage, StreamPhysicalBinding,
        binding_receipt_chain_genesis, claim_attempt_chain_genesis, claim_receipt_chain_genesis,
        management_receipt_chain_genesis,
    };
    use crate::db::manifest::stream_profile::{
        FoldDelegation, ProfileManagementResult, ReceiptChainRef, StreamProfileMode,
        StreamProfileState, stream_profile_management_request_digest,
    };
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
            terminal_dead_letter: None,
        }
    }

    fn authority_row() -> StreamTokenAuthorityRow {
        authority_row_for("person:17", "actor:alice")
    }

    fn profile_receipt(operation_id: &str) -> ProfileManagementReceipt {
        let prior_chain = ReceiptChainRef::genesis();
        let delegation = FoldDelegation::issue(
            "11111111-1111-4111-8111-111111111111",
            format!("sha256:{}", "a".repeat(64)),
            "config-1",
            format!("sha256:{}", "b".repeat(64)),
            2,
            "operator:alice",
            1_700_000_000_000_000,
        )
        .unwrap();
        let result = ProfileManagementResult::new(
            2,
            2,
            StreamProfileState::Enabled {
                active_fold_delegation: delegation,
            },
            0,
            format!("sha256:{}", "c".repeat(64)),
        )
        .unwrap();
        let graph_identity_digest = format!("sha256:{}", "d".repeat(64));
        let declaration_digest = format!("sha256:{}", "b".repeat(64));
        let request_digest = stream_profile_management_request_digest(
            &graph_identity_digest,
            operation_id,
            "config-1",
            &declaration_digest,
            1,
            StreamProfileMode::Enabled,
        )
        .unwrap();
        ProfileManagementReceipt::new(
            graph_identity_digest,
            &prior_chain,
            operation_id,
            request_digest,
            "config-1",
            declaration_digest,
            "operator:alice",
            1,
            result,
            1_700_000_000_000_001,
        )
        .unwrap()
    }

    fn lifecycle_envelope(record_tag: &str, ordinal: usize) -> LifecycleLedgerEnvelope {
        LifecycleLedgerEnvelope {
            record_id: format!("sha256:{:064x}", ordinal + 1),
            record_tag: record_tag.to_string(),
            record_lookup_key: format!("{record_tag}:lookup:{ordinal}"),
            record_payload_json: format!(r#"{{"ordinal":{ordinal}}}"#),
        }
    }

    fn typed_lifecycle_records() -> Vec<LifecycleLedgerRecord> {
        let identity = TableIdentity::new(7, 9).unwrap();
        let graph_digest = format!("sha256:{}", "1".repeat(64));
        let stream_incarnation_id = "22222222-2222-4222-8222-222222222222";
        let binding_scope_id = "33333333-3333-4333-8333-333333333333";
        let enrollment_id = "44444444-4444-4444-8444-444444444444";
        let shard_id = "55555555-5555-4555-8555-555555555555";
        let physical_binding = StreamPhysicalBinding {
            stable_table_id: identity.stable_table_id,
            table_incarnation_id: identity.table_incarnation_id,
            table_location: "nodes/0000000000000007-0000000000000009".to_string(),
            table_branch: None,
            enrollment_id: enrollment_id.to_string(),
            shard_ids: vec![shard_id.to_string()],
            stream_config_version: crate::db::manifest::stream::STREAM_CONFIG_VERSION,
            stream_config_hash: format!("sha256:{}", "2".repeat(64)),
        };
        let enrollment = EnrollmentReceiptV2::new(
            graph_digest.clone(),
            identity,
            &binding_receipt_chain_genesis(),
            "11111111-1111-4111-8111-111111111111",
            format!("sha256:{}", "3".repeat(64)),
            "operator:alice",
            stream_incarnation_id,
            binding_scope_id,
            physical_binding.clone(),
            1_700_000_000_000_001,
        )
        .unwrap();
        let binding = BindingReceipt::new(
            graph_digest.clone(),
            identity,
            &enrollment.next_chain_ref().unwrap(),
            binding_scope_id,
            stream_incarnation_id,
            physical_binding,
            "INITIAL_ENROLLMENT",
            1_700_000_000_000_002,
        )
        .unwrap();
        let management = ManagementReceipt::new(
            graph_digest.clone(),
            identity,
            stream_incarnation_id,
            binding_scope_id,
            &management_receipt_chain_genesis(),
            "66666666-6666-4666-8666-666666666666",
            "QUIESCE",
            1,
            2,
            "operator:alice",
            serde_json::json!({"drain_id":"66666666-6666-4666-8666-666666666666"}),
            serde_json::json!({"lifecycle":"SEALED","revision":2}),
            1_700_000_000_000_003,
        )
        .unwrap();
        let correction = StreamCorrectionReceipt::new(StreamCorrectionReceiptPreimage {
            graph_identity_digest: graph_digest.clone(),
            identity,
            stream_incarnation_id: stream_incarnation_id.to_string(),
            binding_scope_id: binding_scope_id.to_string(),
            block_token: format!("sha256:{}", "4".repeat(64)),
            correction_id: "abababab-abab-4bab-8bab-abababababab".to_string(),
            correction_plan_digest: format!("sha256:{}", "5".repeat(64)),
            actor_id: "operator:alice".to_string(),
            graph_commit_id: "01H000000000000000000000C1".to_string(),
            resulting_manifest_version: 10,
            resulting_lifecycle_revision: 3,
            resulting_lifecycle_digest: format!("sha256:{}", "6".repeat(64)),
            resulting_token_authority_digest: format!("sha256:{}", "7".repeat(64)),
            recorded_at: 1_700_000_000_000_004,
        })
        .unwrap();
        let claim_id = "77777777-7777-4777-8777-777777777777";
        let attempt_id = "88888888-8888-4888-8888-888888888888";
        let tail_prior_position = 0;
        let tail_position = 10;
        let tail_segment_entry_count = tail_position - tail_prior_position;
        let sentinel_digest = format!("sha256:{}", "4".repeat(64));
        let terminal_effect_digest = format!("sha256:{}", "5".repeat(64));
        let attempt = ClaimAttemptEffect::new(
            &claim_attempt_chain_genesis(),
            ClaimAttemptEffectPreimage {
                graph_identity_digest: graph_digest.clone(),
                identity,
                stream_incarnation_id: stream_incarnation_id.to_string(),
                binding_scope_id: binding_scope_id.to_string(),
                enrollment_id: enrollment_id.to_string(),
                shard_id: shard_id.to_string(),
                claim_id: claim_id.to_string(),
                attempt_id: attempt_id.to_string(),
                attempt_plan_digest: format!("sha256:{}", "6".repeat(64)),
                bound_prestate_digest: format!("sha256:{}", "7".repeat(64)),
                storage_envelope_digest: None,
                planned_sentinel_position: tail_position,
                planned_sentinel_digest: sentinel_digest.clone(),
                achieved_shard_manifest_version: Some(2),
                achieved_writer_epoch: Some(2),
                observed_sentinel_position: Some(tail_position),
                observed_sentinel_digest: Some(sentinel_digest.clone()),
                attempt_terminal_effect_digest: terminal_effect_digest.clone(),
                classification: ClaimAttemptClassification::StockManifestPlusSentinel,
            },
        )
        .unwrap();
        let attempt_chain = attempt.next_attempt_chain_ref().unwrap();
        let stream_configuration_digest = format!("sha256:{}", "8".repeat(64));
        let physical_binding_digest = format!("sha256:{}", "9".repeat(64));
        let tail_segment_digest = format!("sha256:{}", "b".repeat(64));
        let tail_empty_fence_digest = format!("sha256:{}", "e".repeat(64));
        let tail_lww_digest = format!("sha256:{}", "f".repeat(64));
        let prior_tail =
            crate::db::manifest::stream::AuthenticatedWalTail::genesis(binding_scope_id).unwrap();
        let tail_chain_digest = crate::db::manifest::stream::authenticated_wal_tail_chain_digest(
            binding_scope_id,
            enrollment_id,
            shard_id,
            stream_incarnation_id,
            &stream_configuration_digest,
            &physical_binding_digest,
            tail_prior_position,
            tail_position,
            tail_segment_entry_count,
            &tail_segment_digest,
            &prior_tail.chain_digest,
            1,
            &tail_empty_fence_digest,
            &tail_lww_digest,
        )
        .unwrap();
        let claim = ClaimReceipt::new(
            &claim_receipt_chain_genesis(),
            ClaimReceiptPreimage {
                graph_identity_digest: graph_digest.clone(),
                identity,
                claim_id: claim_id.to_string(),
                lifecycle_operation_id: Some("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".to_string()),
                binding_scope_id: binding_scope_id.to_string(),
                enrollment_id: enrollment_id.to_string(),
                shard_id: shard_id.to_string(),
                stream_incarnation_id: stream_incarnation_id.to_string(),
                stream_configuration_digest,
                physical_binding_digest,
                recovery_operation_id: "claim-recovery-1".to_string(),
                claim_kind: "DRAIN_FENCE".to_string(),
                profile: ClaimProfile::RetainAll,
                claim_operation_digest: format!("sha256:{}", "a".repeat(64)),
                attempt_count: attempt_chain.record_count,
                attempt_chain_head_id: attempt_chain.head_record_id.unwrap(),
                attempt_effect_chain_digest: attempt_chain.chain_digest,
                terminal_attempt_id: attempt_id.to_string(),
                terminal_pre_shard_manifest_version: 1,
                achieved_shard_manifest_version: 2,
                achieved_writer_epoch: 2,
                sentinel_position: tail_position,
                sentinel_digest,
                replay_cursor: 0,
                authenticated_tail_prior_position: tail_prior_position,
                authenticated_tail_position: tail_position,
                authenticated_tail_published_prefix_position: 0,
                authenticated_tail_segment_entry_count: tail_segment_entry_count,
                authenticated_tail_segment_digest: tail_segment_digest.clone(),
                authenticated_tail_segment_lww_projection_digest: format!(
                    "sha256:{}",
                    "b".repeat(64)
                ),
                authenticated_tail_prior_chain_digest: prior_tail.chain_digest,
                authenticated_tail_segment_count: 1,
                authenticated_tail_chain_digest: tail_chain_digest,
                authenticated_tail_empty_fence_state_digest: tail_empty_fence_digest,
                authenticated_tail_lww_projection_digest: tail_lww_digest,
                terminal_effect_digest,
                terminal_classification: ClaimTerminalClassification::StockManifestPlusSentinel,
                recorded_at: 1_700_000_000_000_004,
            },
        )
        .unwrap();
        let pre_retirement_token_head = CurrentHeadWitness {
            branch_identifier: lance::dataset::refs::BranchIdentifier::main(),
            table_version: 5,
            transaction_uuid: "98989898-9898-4898-8898-989898989898".to_string(),
            manifest_e_tag: None,
        };
        let pre_retirement_token_witness_digest =
            crate::db::manifest::stream_authority_retirement_token_witness_digest(
                &pre_retirement_token_head,
                4,
                1,
            )
            .unwrap();
        let retirement = AuthorityRetirementReceipt::new(
            graph_digest,
            &ReceiptChainRef::genesis(),
            "99999999-9999-4999-8999-999999999999",
            format!("sha256:{}", "c".repeat(64)),
            "operator:alice",
            crate::db::manifest::INTERNAL_MANIFEST_SCHEMA_VERSION,
            7,
            format!("sha256:{}", "d".repeat(64)),
            3,
            format!("sha256:{}", "e".repeat(64)),
            pre_retirement_token_head,
            pre_retirement_token_witness_digest,
            4,
            1,
            format!("sha256:{}", "0".repeat(64)),
            1_700_000_000_000_005,
        )
        .unwrap();
        vec![
            LifecycleLedgerRecord::EnrollmentReceiptV2(enrollment),
            LifecycleLedgerRecord::BindingReceipt(binding),
            LifecycleLedgerRecord::ManagementReceipt(management),
            LifecycleLedgerRecord::StreamCorrectionReceipt(correction),
            LifecycleLedgerRecord::ClaimAttemptEffect(attempt),
            LifecycleLedgerRecord::ClaimReceipt(claim),
            LifecycleLedgerRecord::AuthorityRetirementReceipt(retirement),
        ]
    }

    #[test]
    fn token_authority_row_batch_round_trips_exactly() {
        let row = authority_row();
        let batch = stream_token_rows_to_batch(std::slice::from_ref(&row)).unwrap();
        assert_eq!(stream_token_rows_from_batch(&batch).unwrap(), vec![row]);
        assert_eq!(
            required_string_array(&batch, "record_tag")
                .unwrap()
                .value(0),
            CURRENT_TOKEN_RECORD_TAG
        );
        assert!(
            required_string_array(&batch, "record_payload_json")
                .unwrap()
                .is_null(0)
        );
    }

    #[test]
    fn profile_receipt_batch_is_disjoint_and_round_trips_exactly() {
        let receipt = profile_receipt("profile-operation-1");
        let batch = profile_management_receipt_to_batch(&receipt).unwrap();
        assert_eq!(
            profile_management_receipts_from_batch(&batch).unwrap(),
            vec![receipt]
        );
        let error = stream_token_rows_from_batch(&batch).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("current-token decoder received trusted record tag"),
            "{error}"
        );
    }

    #[test]
    fn lifecycle_ledger_batch_is_disjoint_and_bounded() {
        let first = lifecycle_envelope("STREAM_BINDING_RECEIPT_V1", 0);
        let second = lifecycle_envelope("STREAM_CLAIM_RECEIPT_V1", 1);
        let batch = lifecycle_ledger_envelopes_to_batch(&[first.clone(), second.clone()]).unwrap();
        assert_eq!(
            lifecycle_ledger_envelopes_from_batch(&batch, None).unwrap(),
            vec![first.clone(), second]
        );
        let error = stream_token_rows_from_batch(&batch).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("current-token decoder received trusted record tag"),
            "{error}"
        );
        let error =
            lifecycle_ledger_envelopes_from_batch(&batch, Some(&first.record_tag)).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("lifecycle ledger decoder expected trusted record tag"),
            "{error}"
        );

        let too_many = (0..=MAX_LIFECYCLE_LEDGER_RECORDS_PER_TRANSACTION)
            .map(|ordinal| lifecycle_envelope("STREAM_BINDING_RECEIPT_V1", ordinal))
            .collect::<Vec<_>>();
        let error = lifecycle_ledger_envelopes_to_batch(&too_many).unwrap_err();
        assert!(matches!(
            error,
            OmniError::ResourceLimitExceeded {
                ref resource,
                limit,
                actual,
            } if resource == "stream_lifecycle_ledger_transaction_rows"
                && limit == MAX_LIFECYCLE_LEDGER_RECORDS_PER_TRANSACTION as u64
                && actual == too_many.len() as u64
        ));

        let duplicate_lookup = LifecycleLedgerEnvelope {
            record_id: format!("sha256:{}", "f".repeat(64)),
            ..first.clone()
        };
        let error = lifecycle_ledger_envelopes_to_batch(&[first, duplicate_lookup]).unwrap_err();
        assert!(
            error.to_string().contains("duplicate record lookup key"),
            "{error}"
        );
    }

    #[test]
    fn typed_lifecycle_ledger_records_round_trip_with_disjoint_domains() {
        let records = typed_lifecycle_records();
        let batch = lifecycle_ledger_records_to_batch(&records).unwrap();
        assert_eq!(
            lifecycle_ledger_records_from_batch(&batch).unwrap(),
            records
        );
        let tags = records
            .iter()
            .map(LifecycleLedgerRecord::record_tag)
            .collect::<std::collections::BTreeSet<_>>();
        let lookup_keys = records
            .iter()
            .map(LifecycleLedgerRecord::record_lookup_key)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(tags.len(), records.len());
        assert_eq!(lookup_keys.len(), records.len());
        assert!(!tags.contains(CURRENT_TOKEN_RECORD_TAG));
        assert!(!tags.contains(PROFILE_MANAGEMENT_RECEIPT_TAG));
    }

    #[test]
    fn token_plan_bounds_fail_loudly_for_arrow_and_recovery_json() {
        let row = authority_row();
        let arrow_error =
            validate_stream_token_plan_bounds_with_limits(std::slice::from_ref(&row), 1, u64::MAX)
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

        let json_error =
            validate_stream_token_plan_bounds_with_limits(std::slice::from_ref(&row), u64::MAX, 1)
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

    #[tokio::test]
    async fn staged_profile_receipt_is_immutable_and_receipt_first_lookup_is_exact() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let session = crate::lance_access::control_session();
        let authority = initialize_stream_token_authority(root, &session)
            .await
            .unwrap();
        let dataset = open_stream_token_authority_at(root, &authority, &session)
            .await
            .unwrap();
        assert!(
            dataset
                .load_indices()
                .await
                .unwrap()
                .iter()
                .any(|index| index.name == "stream_control_record_lookup_v1"),
            "v2 initializes the scalar receipt lookup index before manifest selection"
        );
        let receipt = profile_receipt("profile-operation-1");
        assert!(
            lookup_profile_management_receipt(
                &dataset,
                &authority,
                &receipt.graph_identity_digest,
                &receipt.operation_id,
            )
            .await
            .unwrap()
            .is_none()
        );
        let staged = stage_profile_management_receipt(dataset.clone(), &authority, &receipt)
            .await
            .unwrap();
        assert_eq!(dataset.count_rows(None).await.unwrap(), 0);
        let store = crate::table_store::TableStore::new(root, Arc::clone(&session));
        let (achieved, _) = store
            .commit_staged_exact(Arc::new(dataset), staged)
            .await
            .unwrap();
        let next = stream_token_authority_entry_for_dataset(&achieved)
            .await
            .unwrap();
        assert_eq!(
            lookup_profile_management_receipt(
                &achieved,
                &next,
                &receipt.graph_identity_digest,
                &receipt.operation_id,
            )
            .await
            .unwrap(),
            Some(receipt.clone())
        );
        let duplicate = stage_profile_management_receipt(achieved, &next, &receipt).await;
        assert!(
            duplicate.is_err(),
            "WhenMatched::Fail must make immutable receipt rebinding impossible"
        );
    }

    #[tokio::test]
    async fn heterogeneous_lifecycle_ledger_stage_is_atomic_and_immutable() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let session = crate::lance_access::control_session();
        let authority = initialize_stream_token_authority(root, &session)
            .await
            .unwrap();
        let dataset = open_stream_token_authority_at(root, &authority, &session)
            .await
            .unwrap();
        let records = typed_lifecycle_records();
        let enrollment = match &records[0] {
            LifecycleLedgerRecord::EnrollmentReceiptV2(value) => value.clone(),
            _ => unreachable!(),
        };
        let binding = match &records[1] {
            LifecycleLedgerRecord::BindingReceipt(value) => value.clone(),
            _ => unreachable!(),
        };
        let management = match &records[2] {
            LifecycleLedgerRecord::ManagementReceipt(value) => value.clone(),
            _ => unreachable!(),
        };
        let correction = match &records[3] {
            LifecycleLedgerRecord::StreamCorrectionReceipt(value) => value.clone(),
            _ => unreachable!(),
        };
        let attempt = match &records[4] {
            LifecycleLedgerRecord::ClaimAttemptEffect(value) => value.clone(),
            _ => unreachable!(),
        };
        let claim = match &records[5] {
            LifecycleLedgerRecord::ClaimReceipt(value) => value.clone(),
            _ => unreachable!(),
        };
        let retirement = match &records[6] {
            LifecycleLedgerRecord::AuthorityRetirementReceipt(value) => value.clone(),
            _ => unreachable!(),
        };
        assert!(
            lookup_enrollment_receipt_v2(
                &dataset,
                &authority,
                &enrollment.graph_identity_digest,
                enrollment.identity,
                &enrollment.enrollment_request_id,
            )
            .await
            .unwrap()
            .is_none()
        );
        let staged = stage_lifecycle_ledger_records(dataset.clone(), &authority, &records)
            .await
            .unwrap();
        assert_eq!(dataset.count_rows(None).await.unwrap(), 0);

        let store = crate::table_store::TableStore::new(root, Arc::clone(&session));
        let (achieved, _) = store
            .commit_staged_exact(Arc::new(dataset), staged)
            .await
            .unwrap();
        let next = stream_token_authority_entry_for_dataset(&achieved)
            .await
            .unwrap();
        assert_eq!(
            lookup_enrollment_receipt_v2(
                &achieved,
                &next,
                &enrollment.graph_identity_digest,
                enrollment.identity,
                &enrollment.enrollment_request_id,
            )
            .await
            .unwrap(),
            Some(enrollment.clone())
        );
        assert_eq!(
            lookup_binding_receipt(
                &achieved,
                &next,
                &binding.graph_identity_digest,
                binding.identity,
                &binding.binding_scope_id,
                &binding.operation_id,
            )
            .await
            .unwrap(),
            Some(binding.clone())
        );
        assert_eq!(
            lookup_lifecycle_ledger_record_by_id(
                &achieved,
                &next,
                BINDING_RECEIPT_TAG,
                &binding.record_id,
            )
            .await
            .unwrap(),
            Some(LifecycleLedgerRecord::BindingReceipt(binding))
        );
        assert_eq!(
            lookup_management_receipt(
                &achieved,
                &next,
                &management.graph_identity_digest,
                management.identity,
                &management.stream_incarnation_id,
                &management.operation_kind,
                &management.operation_id,
            )
            .await
            .unwrap(),
            Some(management)
        );
        assert_eq!(
            lookup_stream_correction_receipt(
                &achieved,
                &next,
                &correction.graph_identity_digest,
                correction.identity,
                &correction.stream_incarnation_id,
                &correction.block_token,
                &correction.correction_id,
            )
            .await
            .unwrap(),
            Some(correction)
        );
        assert_eq!(
            lookup_claim_attempt_effect(
                &achieved,
                &next,
                &attempt.graph_identity_digest,
                attempt.identity,
                &attempt.binding_scope_id,
                &attempt.claim_id,
                attempt.ordinal,
            )
            .await
            .unwrap(),
            Some(attempt)
        );
        assert_eq!(
            lookup_claim_receipt(
                &achieved,
                &next,
                &claim.graph_identity_digest,
                claim.identity,
                &claim.binding_scope_id,
                &claim.claim_id,
            )
            .await
            .unwrap(),
            Some(claim.clone())
        );
        assert_eq!(
            lookup_lifecycle_ledger_record_by_id(
                &achieved,
                &next,
                CLAIM_RECEIPT_TAG,
                &claim.record_id,
            )
            .await
            .unwrap(),
            Some(LifecycleLedgerRecord::ClaimReceipt(claim))
        );
        assert_eq!(
            lookup_authority_retirement_receipt(
                &achieved,
                &next,
                &retirement.graph_identity_digest,
                &retirement.retirement_id,
            )
            .await
            .unwrap(),
            Some(retirement.clone())
        );
        assert_eq!(
            lookup_lifecycle_ledger_record_by_id(
                &achieved,
                &next,
                AUTHORITY_RETIREMENT_RECEIPT_TAG,
                &retirement.record_id,
            )
            .await
            .unwrap(),
            Some(LifecycleLedgerRecord::AuthorityRetirementReceipt(
                retirement.clone()
            ))
        );
        assert!(
            stage_authority_retirement_receipt(achieved.clone(), &next, &retirement)
                .await
                .is_err(),
            "WhenMatched::Fail must reject rebinding an immutable retirement receipt"
        );
        assert!(
            stage_enrollment_receipt_v2(achieved, &next, &enrollment)
                .await
                .is_err(),
            "WhenMatched::Fail must reject rebinding an immutable lifecycle record"
        );
    }

    #[tokio::test]
    async fn mixed_token_and_correction_receipt_stage_is_one_effect_and_never_rewrites_receipt() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let session = crate::lance_access::control_session();
        let authority = initialize_stream_token_authority(root, &session)
            .await
            .unwrap();
        let dataset = open_stream_token_authority_at(root, &authority, &session)
            .await
            .unwrap();
        let row = authority_row();
        let record = typed_lifecycle_records()
            .into_iter()
            .find(|record| matches!(record, LifecycleLedgerRecord::StreamCorrectionReceipt(_)))
            .unwrap();
        let correction = match &record {
            LifecycleLedgerRecord::StreamCorrectionReceipt(value) => value.clone(),
            _ => unreachable!(),
        };
        let staged = stage_stream_token_and_lifecycle_records(
            dataset.clone(),
            &authority,
            std::slice::from_ref(&row),
            std::slice::from_ref(&record),
        )
        .await
        .unwrap();
        assert_eq!(dataset.count_rows(None).await.unwrap(), 0);
        let store = crate::table_store::TableStore::new(root, Arc::clone(&session));
        let (achieved, _) = store
            .commit_staged_exact(Arc::new(dataset), staged)
            .await
            .unwrap();
        let next = stream_token_authority_entry_for_dataset(&achieved)
            .await
            .unwrap();
        assert_eq!(
            lookup_stream_token_row(&achieved, &next, row.identity, &row.logical_id)
                .await
                .unwrap(),
            Some(row.clone())
        );
        assert_eq!(
            lookup_stream_correction_receipt(
                &achieved,
                &next,
                &correction.graph_identity_digest,
                correction.identity,
                &correction.stream_incarnation_id,
                &correction.block_token,
                &correction.correction_id,
            )
            .await
            .unwrap(),
            Some(correction)
        );
        let error = stage_stream_token_and_lifecycle_records(
            achieved,
            &next,
            std::slice::from_ref(&row),
            std::slice::from_ref(&record),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("immutable lifecycle record id"));
    }
}
