//! Durable RFC-026 stream lifecycle authority.
//!
//! This module defines only the v7 manifest representation. It does not admit
//! rows to MemWAL, claim a writer, or expose a public streaming surface.

use std::collections::BTreeMap;

use lance::dataset::refs::BranchIdentifier;
use lance_index::mem_wal::ShardId;
use serde::{Deserialize, Serialize};

use crate::error::{OmniError, Result};

use super::layout::stream_state_object_id;
use super::{TableIdentity, TableRegistration};

const STREAM_STATE_PROTOCOL_VERSION: u32 = 1;
pub(crate) const STREAM_CONFIG_VERSION: u32 = 1;

/// Stable physical enrollment binding for the bounded RFC-026 profile.
///
/// Identity is repeated in the payload and in the manifest row columns. The
/// decoder requires all copies to agree; it never derives authority from the
/// diagnostic alias or from a compatible-looking path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StreamPhysicalBinding {
    pub(crate) stable_table_id: u64,
    pub(crate) table_incarnation_id: u64,
    pub(crate) table_location: String,
    /// Main is represented canonically as `None`. Named refs are not supported
    /// by the v7 bounded profile.
    pub(crate) table_branch: Option<String>,
    pub(crate) enrollment_id: String,
    /// Sorted, unique UUID namespace. V7 Phase A permits exactly one shard.
    pub(crate) shard_ids: Vec<String>,
    pub(crate) stream_config_version: u32,
    pub(crate) stream_config_hash: String,
}

impl StreamPhysicalBinding {
    pub(crate) fn identity(&self) -> Result<TableIdentity> {
        TableIdentity::new(self.stable_table_id, self.table_incarnation_id)
    }

    fn validate(&self, expected_identity: TableIdentity) -> Result<()> {
        let embedded_identity = self.identity()?;
        if embedded_identity != expected_identity {
            return Err(OmniError::manifest_internal(format!(
                "stream binding identity {embedded_identity} does not match row identity {expected_identity}"
            )));
        }
        if self.table_location.is_empty() || self.table_location.trim() != self.table_location {
            return Err(OmniError::manifest_internal(
                "stream binding table_location must be non-empty and canonical",
            ));
        }
        if self.table_branch.is_some() {
            return Err(OmniError::manifest_internal(
                "internal schema v7 stream bindings support only canonical main (table_branch = null)",
            ));
        }
        let enrollment_id = validate_uuid("enrollment_id", &self.enrollment_id)?;
        if enrollment_id.get_version_num() != 4 {
            return Err(OmniError::manifest_internal(
                "stream enrollment_id must be a UUID v4 value",
            ));
        }
        if self.shard_ids.len() != 1 {
            return Err(OmniError::manifest_internal(format!(
                "internal schema v7 requires exactly one stream shard, got {}",
                self.shard_ids.len()
            )));
        }
        let mut prior: Option<&str> = None;
        for shard_id in &self.shard_ids {
            let parsed = validate_uuid("shard_id", shard_id)?;
            if parsed.get_version_num() != 4 {
                return Err(OmniError::manifest_internal(
                    "stream shard_id must be a UUID v4 value",
                ));
            }
            if parsed == enrollment_id {
                return Err(OmniError::manifest_internal(
                    "stream enrollment_id and shard_id must be distinct identities",
                ));
            }
            if prior.is_some_and(|prior| prior >= shard_id.as_str()) {
                return Err(OmniError::manifest_internal(
                    "stream shard_ids must be strictly sorted and unique",
                ));
            }
            prior = Some(shard_id);
        }
        if self.stream_config_version != STREAM_CONFIG_VERSION {
            return Err(OmniError::manifest_internal(format!(
                "unsupported stream config version {}, expected {}",
                self.stream_config_version, STREAM_CONFIG_VERSION
            )));
        }
        let Some(config_digest) = self.stream_config_hash.strip_prefix("sha256:") else {
            return Err(OmniError::manifest_internal(
                "stream_config_hash must use canonical sha256:<lowercase-hex> form",
            ));
        };
        if config_digest.len() != 64
            || !config_digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(OmniError::manifest_internal(
                "stream_config_hash must contain exactly 64 lowercase hexadecimal digits",
            ));
        }
        Ok(())
    }
}

/// Exact public Lance witness for the currently accepted physical table HEAD.
/// This changes on every base-table commit and is not enrollment identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CurrentHeadWitness {
    pub(crate) branch_identifier: BranchIdentifier,
    pub(crate) table_version: u64,
    pub(crate) transaction_uuid: String,
    pub(crate) manifest_e_tag: Option<String>,
}

impl CurrentHeadWitness {
    fn validate(&self) -> Result<()> {
        if self.branch_identifier != BranchIdentifier::main() {
            return Err(OmniError::manifest_internal(
                "internal schema v7 stream HEAD witness must name the main branch",
            ));
        }
        if self.table_version == 0 {
            return Err(OmniError::manifest_internal(
                "stream HEAD witness table_version must be non-zero",
            ));
        }
        validate_uuid("transaction_uuid", &self.transaction_uuid)?;
        if self
            .manifest_e_tag
            .as_ref()
            .is_some_and(|e_tag| e_tag.is_empty() || e_tag.trim() != e_tag)
        {
            return Err(OmniError::manifest_internal(
                "stream HEAD witness manifest_e_tag must be absent or non-empty canonical text",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum StreamLifecycle {
    Open,
    Draining,
    Sealed,
}

impl StreamLifecycle {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Open => "OPEN",
            Self::Draining => "DRAINING",
            Self::Sealed => "SEALED",
        }
    }
}

/// One materialized `stream_state:<stable>:<incarnation>` authority row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StreamLifecycleEntry {
    pub(crate) identity: TableIdentity,
    /// Human-readable only. It may lag a metadata-only rename and must never be
    /// used to locate or adopt physical stream state.
    pub(crate) diagnostic_table_key: String,
    pub(crate) lifecycle: StreamLifecycle,
    pub(crate) binding: StreamPhysicalBinding,
    pub(crate) current_head_witness: CurrentHeadWitness,
    /// Epochs are scoped to the binding's never-reused shard IDs.
    pub(crate) epoch_floor_by_shard: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StreamStatePayload {
    protocol_version: u32,
    stable_table_id: u64,
    table_incarnation_id: u64,
    lifecycle: StreamLifecycle,
    binding: StreamPhysicalBinding,
    current_head_witness: CurrentHeadWitness,
    epoch_floor_by_shard: BTreeMap<String, u64>,
}

impl StreamLifecycleEntry {
    pub(crate) fn object_id(&self) -> String {
        stream_state_object_id(self.identity)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        self.identity.validate()?;
        if self.diagnostic_table_key.is_empty() {
            return Err(OmniError::manifest_internal(
                "stream lifecycle diagnostic table key must be non-empty",
            ));
        }
        self.binding.validate(self.identity)?;
        self.current_head_witness.validate()?;

        if self.epoch_floor_by_shard.len() != self.binding.shard_ids.len() {
            return Err(OmniError::manifest_internal(
                "stream epoch-floor keys must exactly match the physical shard binding",
            ));
        }
        for shard_id in &self.binding.shard_ids {
            let epoch = self.epoch_floor_by_shard.get(shard_id).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "stream epoch floor missing bound shard {shard_id}"
                ))
            })?;
            if *epoch == 0 {
                return Err(OmniError::manifest_internal(format!(
                    "stream epoch floor for shard {shard_id} must record a claimed non-zero epoch"
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn validate_against_registration(
        &self,
        registration: &TableRegistration,
    ) -> Result<()> {
        self.validate()?;
        if self.identity != registration.identity {
            return Err(OmniError::manifest_internal(format!(
                "stream lifecycle identity {} does not match table registration {}",
                self.identity, registration.identity
            )));
        }
        if self.binding.table_location != registration.table_path {
            return Err(OmniError::manifest_internal(format!(
                "stream lifecycle for identity {} binds location '{}', registered location is '{}'",
                self.identity, self.binding.table_location, registration.table_path
            )));
        }
        Ok(())
    }

    pub(super) fn to_metadata_json(&self) -> Result<String> {
        self.validate()?;
        serde_json::to_string(&StreamStatePayload {
            protocol_version: STREAM_STATE_PROTOCOL_VERSION,
            stable_table_id: self.identity.stable_table_id,
            table_incarnation_id: self.identity.table_incarnation_id,
            lifecycle: self.lifecycle,
            binding: self.binding.clone(),
            current_head_witness: self.current_head_witness.clone(),
            epoch_floor_by_shard: self.epoch_floor_by_shard.clone(),
        })
        .map_err(|error| {
            OmniError::manifest_internal(format!(
                "failed to encode stream lifecycle metadata: {error}"
            ))
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn from_manifest_row(
        object_id: &str,
        diagnostic_table_key: &str,
        row_identity: TableIdentity,
        location: Option<&str>,
        table_version: Option<u64>,
        table_branch: Option<&str>,
        metadata_json: &str,
    ) -> Result<Self> {
        let expected_object_id = stream_state_object_id(row_identity);
        if object_id != expected_object_id {
            return Err(OmniError::manifest_internal(format!(
                "manifest stream_state row has object_id '{object_id}', expected '{expected_object_id}'"
            )));
        }
        let payload: StreamStatePayload = serde_json::from_str(metadata_json).map_err(|error| {
            OmniError::manifest_internal(format!(
                "failed to decode stream lifecycle metadata: {error}"
            ))
        })?;
        if payload.protocol_version != STREAM_STATE_PROTOCOL_VERSION {
            return Err(OmniError::manifest_internal(format!(
                "unsupported stream lifecycle payload version {}, expected {}",
                payload.protocol_version, STREAM_STATE_PROTOCOL_VERSION
            )));
        }
        let payload_identity =
            TableIdentity::new(payload.stable_table_id, payload.table_incarnation_id)?;
        if payload_identity != row_identity {
            return Err(OmniError::manifest_internal(format!(
                "stream lifecycle payload identity {payload_identity} does not match row identity {row_identity}"
            )));
        }
        let entry = Self {
            identity: row_identity,
            diagnostic_table_key: diagnostic_table_key.to_string(),
            lifecycle: payload.lifecycle,
            binding: payload.binding,
            current_head_witness: payload.current_head_witness,
            epoch_floor_by_shard: payload.epoch_floor_by_shard,
        };
        entry.validate()?;
        if location != Some(entry.binding.table_location.as_str()) {
            return Err(OmniError::manifest_internal(
                "stream lifecycle row location does not match its physical binding",
            ));
        }
        if table_version != Some(entry.current_head_witness.table_version) {
            return Err(OmniError::manifest_internal(
                "stream lifecycle row table_version does not match its HEAD witness",
            ));
        }
        if table_branch != entry.binding.table_branch.as_deref() {
            return Err(OmniError::manifest_internal(
                "stream lifecycle row table_branch does not match its physical binding",
            ));
        }
        Ok(entry)
    }
}

fn validate_uuid(field: &str, value: &str) -> Result<ShardId> {
    let parsed = ShardId::parse_str(value).map_err(|error| {
        OmniError::manifest_internal(format!("stream {field} is not a UUID: {error}"))
    })?;
    if parsed.is_nil() {
        return Err(OmniError::manifest_internal(format!(
            "stream {field} must be non-nil"
        )));
    }
    if parsed.to_string() != value {
        return Err(OmniError::manifest_internal(format!(
            "stream {field} must use canonical lowercase hyphenated UUID text"
        )));
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry() -> StreamLifecycleEntry {
        let shard_id = "22222222-2222-4222-8222-222222222222".to_string();
        StreamLifecycleEntry {
            identity: TableIdentity::new(7, 9).unwrap(),
            diagnostic_table_key: "node:Person".to_string(),
            lifecycle: StreamLifecycle::Open,
            binding: StreamPhysicalBinding {
                stable_table_id: 7,
                table_incarnation_id: 9,
                table_location: "nodes/0000000000000007-0000000000000009".to_string(),
                table_branch: None,
                enrollment_id: "11111111-1111-4111-8111-111111111111".to_string(),
                shard_ids: vec![shard_id.clone()],
                stream_config_version: STREAM_CONFIG_VERSION,
                stream_config_hash: format!("sha256:{}", "a".repeat(64)),
            },
            current_head_witness: CurrentHeadWitness {
                branch_identifier: BranchIdentifier::main(),
                table_version: 4,
                transaction_uuid: "33333333-3333-4333-8333-333333333333".to_string(),
                manifest_e_tag: None,
            },
            epoch_floor_by_shard: BTreeMap::from([(shard_id, 1)]),
        }
    }

    #[test]
    fn lifecycle_payload_round_trips_deterministically_without_alias_authority() {
        let entry = entry();
        let json = entry.to_metadata_json().unwrap();
        assert_eq!(json, entry.to_metadata_json().unwrap());
        let decoded = StreamLifecycleEntry::from_manifest_row(
            &entry.object_id(),
            "node:RenamedPerson",
            entry.identity,
            Some(&entry.binding.table_location),
            Some(entry.current_head_witness.table_version),
            None,
            &json,
        )
        .unwrap();
        assert_eq!(decoded.identity, entry.identity);
        assert_eq!(decoded.diagnostic_table_key, "node:RenamedPerson");
        assert_eq!(decoded.binding, entry.binding);

        assert!(
            StreamLifecycleEntry::from_manifest_row(
                "stream_state:wrong",
                &entry.diagnostic_table_key,
                entry.identity,
                Some(&entry.binding.table_location),
                Some(entry.current_head_witness.table_version),
                None,
                &json,
            )
            .is_err()
        );
        assert!(
            StreamLifecycleEntry::from_manifest_row(
                &entry.object_id(),
                &entry.diagnostic_table_key,
                entry.identity,
                Some(&entry.binding.table_location),
                Some(entry.current_head_witness.table_version + 1),
                None,
                &json,
            )
            .is_err()
        );
    }

    #[test]
    fn lifecycle_payload_rejects_identity_branch_and_epoch_ambiguity() {
        let mut wrong_identity = entry();
        wrong_identity.binding.stable_table_id += 1;
        assert!(wrong_identity.validate().is_err());

        let mut named = entry();
        named.binding.table_branch = Some("feature".to_string());
        assert!(named.validate().is_err());

        let mut missing_epoch = entry();
        missing_epoch.epoch_floor_by_shard.clear();
        assert!(missing_epoch.validate().is_err());

        let mut empty_etag = entry();
        empty_etag.current_head_witness.manifest_e_tag = Some(String::new());
        assert!(empty_etag.validate().is_err());

        let mut malformed_hash = entry();
        malformed_hash.binding.stream_config_hash = "sha256:not-a-digest".to_string();
        assert!(malformed_hash.validate().is_err());
    }
}
