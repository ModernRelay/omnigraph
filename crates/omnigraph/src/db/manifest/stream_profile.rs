//! Graph-global RFC-026 §4.7 stream-profile enablement authority.
//!
//! One required `__manifest` singleton row records whether the experimental
//! streaming profile is enabled for this graph. The flag is a graph-wide
//! safety property (the stream freeze depends on every writer knowing streams
//! may exist), so it can never live in per-process configuration: every open
//! path decodes this row, and it moves only through the shared publisher's
//! exact-entry CAS (`ManifestChange::SetStreamProfile`).
//!
//! The row exists from genesis (disabled, revision 1). Internal schema v10 is
//! the first format that carries it; v9 decoders skip unknown row kinds
//! silently, which is exactly the disobedience the stamp exists to prevent —
//! see `migrations.rs`.

use serde::{Deserialize, Serialize};

use super::layout::stream_profile_object_id;
use super::stream::deserialize_present_option;
use crate::error::{OmniError, Result};

/// Exact-equality wire gate for the stream-profile payload, mirroring the
/// token-authority pattern: an unknown payload version is refused, never
/// reinterpreted through serde defaults.
pub(crate) const STREAM_PROFILE_PROTOCOL_VERSION: u32 = 1;

/// The single graph-global stream-profile row materialized in `__manifest`.
///
/// Column shape (validated on both encode and decode): empty `table_key`,
/// null identity, null `location`, null `table_version`, null `table_branch`,
/// null `row_count` — the flag is pure metadata with no physical participant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StreamProfileEntry {
    pub(crate) streaming_enabled: bool,
    /// Reserved pending-disable slot. Protocol v1 requires it physically
    /// present and null: no code path can set it, and activating a durable
    /// pending-disable state requires a protocol-version bump rather than a
    /// serde-default reinterpretation of already-stamped rows.
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) disable_pending_since: Option<i64>,
    /// Monotonic CAS revision. Every accepted `SetStreamProfile` must advance
    /// it strictly, so a stale expected entry can never silently overwrite a
    /// concurrent transition.
    pub(crate) profile_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StreamProfilePayload {
    protocol_version: u32,
    streaming_enabled: bool,
    #[serde(deserialize_with = "deserialize_present_option")]
    disable_pending_since: Option<i64>,
    profile_revision: u64,
}

impl StreamProfileEntry {
    /// The genesis profile every fresh graph is initialized with: streaming
    /// disabled, revision 1, no pending disable.
    pub(crate) fn genesis() -> Self {
        Self {
            streaming_enabled: false,
            disable_pending_since: None,
            profile_revision: 1,
        }
    }

    pub(crate) fn object_id(&self) -> &'static str {
        stream_profile_object_id()
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.profile_revision == 0 {
            return Err(OmniError::manifest_internal(
                "stream profile revision must be positive (genesis starts at 1)",
            ));
        }
        if self.disable_pending_since.is_some() {
            return Err(OmniError::manifest_internal(
                "stream profile protocol v1 reserves disable_pending_since and requires it null",
            ));
        }
        Ok(())
    }

    pub(super) fn to_metadata_json(&self) -> Result<String> {
        self.validate()?;
        serde_json::to_string(&StreamProfilePayload {
            protocol_version: STREAM_PROFILE_PROTOCOL_VERSION,
            streaming_enabled: self.streaming_enabled,
            disable_pending_since: self.disable_pending_since,
            profile_revision: self.profile_revision,
        })
        .map_err(|error| {
            OmniError::manifest_internal(format!(
                "failed to encode stream-profile metadata: {error}"
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
        let expected_object_id = stream_profile_object_id();
        if object_id != expected_object_id {
            return Err(OmniError::manifest_internal(format!(
                "manifest stream_profile row has object_id '{object_id}', expected '{expected_object_id}'"
            )));
        }
        if location.is_some() || table_version.is_some() || table_branch.is_some() {
            return Err(OmniError::manifest_internal(
                "manifest stream_profile row must have null location, table_version, and table_branch",
            ));
        }
        let payload: StreamProfilePayload =
            serde_json::from_str(metadata_json).map_err(|error| {
                OmniError::manifest_internal(format!(
                    "failed to decode stream-profile metadata: {error}"
                ))
            })?;
        if payload.protocol_version != STREAM_PROFILE_PROTOCOL_VERSION {
            return Err(OmniError::manifest_internal(format!(
                "unsupported stream-profile payload version {}, expected {}",
                payload.protocol_version, STREAM_PROFILE_PROTOCOL_VERSION
            )));
        }
        let entry = Self {
            streaming_enabled: payload.streaming_enabled,
            disable_pending_since: payload.disable_pending_since,
            profile_revision: payload.profile_revision,
        };
        entry.validate()?;
        Ok(entry)
    }
}

/// Public projection of the graph's streaming posture, computed from state a
/// [`super::Snapshot`] already holds (zero extra I/O).
///
/// `undrained` is true while any stream lifecycle row is non-terminal — the
/// exact condition that makes a disable refuse as pending-until-drained. It is
/// surfaced now so operator observation (`cluster status` / refresh) can
/// report drain state without a later engine-API change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamingStatus {
    pub enabled: bool,
    pub undrained: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn genesis_is_disabled_at_revision_one_and_round_trips() {
        let genesis = StreamProfileEntry::genesis();
        assert!(!genesis.streaming_enabled);
        assert_eq!(genesis.profile_revision, 1);
        assert_eq!(genesis.disable_pending_since, None);

        let json = genesis.to_metadata_json().expect("genesis encodes");
        // The reserved slot is physically present as an explicit null, and the
        // wire payload carries the exact-equality protocol gate.
        assert!(json.contains("\"disable_pending_since\":null"), "got: {json}");
        assert!(json.contains("\"protocol_version\":1"), "got: {json}");

        let decoded = StreamProfileEntry::from_manifest_row(
            stream_profile_object_id(),
            None,
            None,
            None,
            &json,
        )
        .expect("genesis decodes");
        assert_eq!(decoded, genesis);
    }

    #[test]
    fn decode_refuses_unknown_keys_absent_slots_and_wrong_versions() {
        // Unknown key: deny_unknown_fields is load-bearing format truth.
        let unknown = r#"{"protocol_version":1,"streaming_enabled":true,"disable_pending_since":null,"profile_revision":2,"extra":1}"#;
        let err = StreamProfileEntry::from_manifest_row(
            stream_profile_object_id(),
            None,
            None,
            None,
            unknown,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("failed to decode stream-profile metadata"), "got: {err}");

        // Absent reserved slot: the key must be physically present — a
        // truncated or hand-built row is never reinterpreted by a default.
        let absent = r#"{"protocol_version":1,"streaming_enabled":true,"profile_revision":2}"#;
        let err = StreamProfileEntry::from_manifest_row(
            stream_profile_object_id(),
            None,
            None,
            None,
            absent,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("failed to decode stream-profile metadata"), "got: {err}");

        // Wrong protocol version: exact-equality gate, never a default.
        let wrong = r#"{"protocol_version":2,"streaming_enabled":true,"disable_pending_since":null,"profile_revision":2}"#;
        let err = StreamProfileEntry::from_manifest_row(
            stream_profile_object_id(),
            None,
            None,
            None,
            wrong,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("unsupported stream-profile payload version 2"), "got: {err}");

        // A populated reserved slot is refused under protocol v1.
        let pending = r#"{"protocol_version":1,"streaming_enabled":true,"disable_pending_since":42,"profile_revision":2}"#;
        let err = StreamProfileEntry::from_manifest_row(
            stream_profile_object_id(),
            None,
            None,
            None,
            pending,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("reserves disable_pending_since"), "got: {err}");

        // Column-borne physical fields must all be null: the flag has no
        // physical participant.
        let json = StreamProfileEntry::genesis().to_metadata_json().unwrap();
        let err = StreamProfileEntry::from_manifest_row(
            stream_profile_object_id(),
            Some("somewhere"),
            None,
            None,
            &json,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("null location"), "got: {err}");

        let err = StreamProfileEntry::from_manifest_row("stream_profiles", None, None, None, &json)
            .unwrap_err()
            .to_string();
        assert!(err.contains("expected 'stream_profile'"), "got: {err}");
    }
}
