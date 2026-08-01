//! Graph-global RFC-026 stream-profile authority.
//!
//! Internal manifest v11 replaces the old boolean flag with a discriminated
//! authority state. The hot manifest row remains bounded: immutable management
//! history lives in the manifest-selected stream-token ledger and the profile
//! retains only one fixed-size chain commitment.

use lance_index::mem_wal::ShardId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::layout::stream_profile_object_id;
use crate::error::{OmniError, Result};

/// Exact-equality wire gate for the v11 stream-profile payload.
pub(crate) const STREAM_PROFILE_PROTOCOL_VERSION: u32 = 2;
pub(crate) const STREAM_FOLD_PRINCIPAL: &str = "omnigraph:stream-fold";

const SHA256_PREFIX: &str = "sha256:";
const MAX_PROFILE_TEXT_BYTES: usize = 4 * 1024;
const RECEIPT_CHAIN_GENESIS_DOMAIN: &[u8] = b"omnigraph.stream-profile-receipt-chain.genesis.v1\0";
const FOLD_DELEGATION_DOMAIN: &[u8] = b"omnigraph.stream-fold-delegation.v1\0";
const FOLD_CONTINUATION_DOMAIN: &[u8] = b"omnigraph.stream-fold-continuation.v1\0";
const PROFILE_REQUEST_DOMAIN: &[u8] = b"omnigraph.stream-profile-request.v1\0";
const PROFILE_RECEIPT_LOOKUP_DOMAIN: &[u8] = b"omnigraph.stream-profile-receipt-lookup.v1\0";
const PROFILE_RECEIPT_RECORD_DOMAIN: &[u8] = b"omnigraph.stream-profile-receipt-record.v1\0";
const PROFILE_RECEIPT_RESULT_DOMAIN: &[u8] = b"omnigraph.stream-profile-receipt-result.v1\0";
const PROFILE_RECEIPT_CHAIN_DOMAIN: &[u8] = b"omnigraph.stream-profile-receipt-chain.v1\0";
const AUTHORITY_RETIREMENT_REQUEST_DOMAIN: &[u8] =
    b"omnigraph.stream-authority-retirement-request.v1\0";
const AUTHORITY_RETIREMENT_RECEIPT_LOOKUP_DOMAIN: &[u8] =
    b"omnigraph.stream-authority-retirement-receipt-lookup.v1\0";
const AUTHORITY_RETIREMENT_RECEIPT_RECORD_DOMAIN: &[u8] =
    b"omnigraph.stream-authority-retirement-receipt-record.v1\0";
const AUTHORITY_RETIREMENT_RECEIPT_CHAIN_DOMAIN: &[u8] =
    b"omnigraph.stream-authority-retirement-receipt-chain.v1\0";
const AUTHORITY_RETIREMENT_TOKEN_WITNESS_DOMAIN: &[u8] =
    b"omnigraph.stream-authority-retirement-token-witness.v1\0";
pub(crate) const PROFILE_MANAGEMENT_RECEIPT_PROTOCOL_VERSION: u32 = 1;
pub(crate) const PROFILE_MANAGEMENT_RECEIPT_TAG: &str = "PROFILE_MANAGEMENT_RECEIPT_V1";
pub(crate) const AUTHORITY_RETIREMENT_RECEIPT_PROTOCOL_VERSION: u32 = 1;
pub(crate) const AUTHORITY_RETIREMENT_RECEIPT_TAG: &str = "AUTHORITY_RETIREMENT_RECEIPT_V1";

/// Fixed-size commitment to immutable profile-management receipt history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReceiptChainRef {
    pub(crate) head_record_id: Option<String>,
    pub(crate) record_count: u64,
    pub(crate) chain_digest: String,
}

impl ReceiptChainRef {
    pub(crate) fn genesis() -> Self {
        Self::genesis_with_domain(RECEIPT_CHAIN_GENESIS_DOMAIN)
    }

    /// Construct the canonical empty commitment for another receipt family.
    ///
    /// The wire shape is shared across the profile and lane-scoped ledgers,
    /// while each family supplies its own domain so an empty chain cannot be
    /// transplanted between authorities.
    pub(crate) fn genesis_with_domain(domain: &[u8]) -> Self {
        Self {
            head_record_id: None,
            record_count: 0,
            chain_digest: sha256(domain),
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        self.validate_with_domain(RECEIPT_CHAIN_GENESIS_DOMAIN)
    }

    /// Validate this fixed-size reference against one receipt family's exact
    /// genesis commitment. Non-empty chains still carry a deterministic
    /// SHA-256 head and cumulative digest, independent of the family.
    pub(crate) fn validate_with_domain(&self, genesis_domain: &[u8]) -> Result<()> {
        validate_sha256("profile receipt-chain digest", &self.chain_digest)?;
        match (self.record_count, self.head_record_id.as_deref()) {
            (0, None) if self.chain_digest == sha256(genesis_domain) => Ok(()),
            (0, _) => Err(profile_error(
                "an empty profile receipt chain must use the canonical genesis commitment",
            )),
            (_, None) => Err(profile_error(
                "a non-empty profile receipt chain requires a head record id",
            )),
            (_, Some(head)) => validate_sha256("profile receipt-chain head", head),
        }
    }
}

/// Durable profile mode. Keeping this separate from the payload-bearing state
/// gives callers an unambiguous fail-closed switch without projection booleans.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum StreamProfileMode {
    Disabled,
    Enabled,
    Disabling,
    Retired,
}

impl StreamProfileMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "DISABLED",
            Self::Enabled => "ENABLED",
            Self::Disabling => "DISABLING",
            Self::Retired => "RETIRED",
        }
    }
}

/// Bounded delegation installed by an enable publication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FoldDelegation {
    pub(crate) delegation_id: String,
    pub(crate) cluster_root_digest: String,
    pub(crate) declaration_revision: String,
    pub(crate) declaration_digest: String,
    pub(crate) enabled_profile_revision: u64,
    pub(crate) issuing_actor: String,
    pub(crate) principal: String,
    pub(crate) issued_at: i64,
    pub(crate) delegation_digest: String,
}

impl FoldDelegation {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn issue(
        delegation_id: impl Into<String>,
        cluster_root_digest: impl Into<String>,
        declaration_revision: impl Into<String>,
        declaration_digest: impl Into<String>,
        enabled_profile_revision: u64,
        issuing_actor: impl Into<String>,
        issued_at: i64,
    ) -> Result<Self> {
        let mut value = Self {
            delegation_id: delegation_id.into(),
            cluster_root_digest: cluster_root_digest.into(),
            declaration_revision: declaration_revision.into(),
            declaration_digest: declaration_digest.into(),
            enabled_profile_revision,
            issuing_actor: issuing_actor.into(),
            principal: STREAM_FOLD_PRINCIPAL.to_string(),
            issued_at,
            delegation_digest: String::new(),
        };
        value.delegation_digest = value.compute_digest();
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_canonical_uuid("fold delegation id", &self.delegation_id)?;
        validate_sha256(
            "fold delegation cluster-root digest",
            &self.cluster_root_digest,
        )?;
        validate_profile_text(
            "fold delegation declaration revision",
            &self.declaration_revision,
        )?;
        validate_sha256(
            "fold delegation declaration digest",
            &self.declaration_digest,
        )?;
        if self.enabled_profile_revision == 0 {
            return Err(profile_error(
                "fold delegation enabled profile revision must be positive",
            ));
        }
        validate_profile_text("fold delegation issuing actor", &self.issuing_actor)?;
        if self.principal != STREAM_FOLD_PRINCIPAL {
            return Err(profile_error(format!(
                "fold delegation principal must be '{STREAM_FOLD_PRINCIPAL}'"
            )));
        }
        if self.issued_at <= 0 {
            return Err(profile_error(
                "fold delegation issued_at must be a positive timestamp",
            ));
        }
        validate_sha256("fold delegation digest", &self.delegation_digest)?;
        if self.delegation_digest != self.compute_digest() {
            return Err(profile_error(
                "fold delegation digest differs from its canonical preimage",
            ));
        }
        Ok(())
    }

    fn compute_digest(&self) -> String {
        hash_fields(
            FOLD_DELEGATION_DOMAIN,
            &[
                self.delegation_id.as_bytes(),
                self.cluster_root_digest.as_bytes(),
                self.declaration_revision.as_bytes(),
                self.declaration_digest.as_bytes(),
                &self.enabled_profile_revision.to_be_bytes(),
                self.issuing_actor.as_bytes(),
                self.principal.as_bytes(),
                &self.issued_at.to_be_bytes(),
            ],
        )
    }
}

/// Drain-only authority derived from, and narrower than, the consumed live
/// fold delegation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FoldContinuation {
    pub(crate) source_delegation_id: String,
    pub(crate) source_delegation_digest: String,
    pub(crate) cluster_root_digest: String,
    pub(crate) declaration_digest: String,
    pub(crate) disabling_profile_revision: u64,
    pub(crate) principal: String,
    pub(crate) continuation_digest: String,
}

impl FoldContinuation {
    pub(crate) fn derive(
        delegation: &FoldDelegation,
        disabling_profile_revision: u64,
    ) -> Result<Self> {
        delegation.validate()?;
        let mut value = Self {
            source_delegation_id: delegation.delegation_id.clone(),
            source_delegation_digest: delegation.delegation_digest.clone(),
            cluster_root_digest: delegation.cluster_root_digest.clone(),
            declaration_digest: delegation.declaration_digest.clone(),
            disabling_profile_revision,
            principal: STREAM_FOLD_PRINCIPAL.to_string(),
            continuation_digest: String::new(),
        };
        value.continuation_digest = value.compute_digest();
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_canonical_uuid(
            "fold continuation source delegation id",
            &self.source_delegation_id,
        )?;
        validate_sha256(
            "fold continuation source delegation digest",
            &self.source_delegation_digest,
        )?;
        validate_sha256(
            "fold continuation cluster-root digest",
            &self.cluster_root_digest,
        )?;
        validate_sha256(
            "fold continuation declaration digest",
            &self.declaration_digest,
        )?;
        if self.disabling_profile_revision == 0 {
            return Err(profile_error(
                "fold continuation disabling profile revision must be positive",
            ));
        }
        if self.principal != STREAM_FOLD_PRINCIPAL {
            return Err(profile_error(format!(
                "fold continuation principal must be '{STREAM_FOLD_PRINCIPAL}'"
            )));
        }
        validate_sha256("fold continuation digest", &self.continuation_digest)?;
        if self.continuation_digest != self.compute_digest() {
            return Err(profile_error(
                "fold continuation digest differs from its canonical preimage",
            ));
        }
        Ok(())
    }

    fn compute_digest(&self) -> String {
        hash_fields(
            FOLD_CONTINUATION_DOMAIN,
            &[
                self.source_delegation_id.as_bytes(),
                self.source_delegation_digest.as_bytes(),
                self.cluster_root_digest.as_bytes(),
                self.declaration_digest.as_bytes(),
                &self.disabling_profile_revision.to_be_bytes(),
                self.principal.as_bytes(),
            ],
        )
    }
}

/// Durable ownership of an in-progress disable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DisablePlan {
    pub(crate) operation_id: String,
    pub(crate) request_digest: String,
    pub(crate) declaration_revision: String,
    pub(crate) declaration_digest: String,
    pub(crate) actor: String,
    pub(crate) fold_continuation: FoldContinuation,
}

impl DisablePlan {
    pub(crate) fn new(
        operation_id: impl Into<String>,
        request_digest: impl Into<String>,
        declaration_revision: impl Into<String>,
        declaration_digest: impl Into<String>,
        actor: impl Into<String>,
        fold_continuation: FoldContinuation,
    ) -> Result<Self> {
        let value = Self {
            operation_id: operation_id.into(),
            request_digest: request_digest.into(),
            declaration_revision: declaration_revision.into(),
            declaration_digest: declaration_digest.into(),
            actor: actor.into(),
            fold_continuation,
        };
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_profile_text("disable operation id", &self.operation_id)?;
        validate_sha256("disable request digest", &self.request_digest)?;
        validate_profile_text("disable declaration revision", &self.declaration_revision)?;
        validate_sha256("disable declaration digest", &self.declaration_digest)?;
        validate_profile_text("disable actor", &self.actor)?;
        // The plan binds the desired disabled declaration. The continuation
        // independently binds the consumed source-enabled declaration; those
        // digests normally differ because the declaration's enabled bit moved.
        self.fold_continuation.validate()
    }
}

/// Exact payload-bearing profile state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub(crate) enum StreamProfileState {
    Disabled,
    Enabled {
        active_fold_delegation: FoldDelegation,
    },
    Disabling {
        disable_plan: DisablePlan,
    },
    Retired {
        authority_retirement_id: String,
        authority_retirement_receipt_id: String,
        authority_retirement_cut_digest: String,
    },
}

impl StreamProfileState {
    pub(crate) fn mode(&self) -> StreamProfileMode {
        match self {
            Self::Disabled => StreamProfileMode::Disabled,
            Self::Enabled { .. } => StreamProfileMode::Enabled,
            Self::Disabling { .. } => StreamProfileMode::Disabling,
            Self::Retired { .. } => StreamProfileMode::Retired,
        }
    }

    fn validate(&self, profile_revision: u64) -> Result<()> {
        match self {
            Self::Disabled => Ok(()),
            Self::Enabled {
                active_fold_delegation,
            } => {
                active_fold_delegation.validate()?;
                if active_fold_delegation.enabled_profile_revision != profile_revision {
                    return Err(profile_error(
                        "active fold delegation is bound to another profile revision",
                    ));
                }
                Ok(())
            }
            Self::Disabling { disable_plan } => {
                disable_plan.validate()?;
                if disable_plan.fold_continuation.disabling_profile_revision != profile_revision {
                    return Err(profile_error(
                        "disable continuation is bound to another profile revision",
                    ));
                }
                Ok(())
            }
            Self::Retired {
                authority_retirement_id,
                authority_retirement_receipt_id,
                authority_retirement_cut_digest,
            } => {
                validate_canonical_uuid("authority retirement id", authority_retirement_id)?;
                validate_sha256(
                    "authority retirement receipt id",
                    authority_retirement_receipt_id,
                )?;
                validate_sha256(
                    "authority retirement cut digest",
                    authority_retirement_cut_digest,
                )
            }
        }
    }
}

/// The single graph-global stream-profile row materialized in `__manifest`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StreamProfileEntry {
    pub(crate) profile_revision: u64,
    pub(crate) profile_receipt_chain: ReceiptChainRef,
    pub(crate) state: StreamProfileState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StreamProfilePayload {
    protocol_version: u32,
    profile_revision: u64,
    profile_receipt_chain: ReceiptChainRef,
    state: StreamProfileState,
}

impl StreamProfileEntry {
    pub(crate) fn genesis() -> Self {
        Self {
            profile_revision: 1,
            profile_receipt_chain: ReceiptChainRef::genesis(),
            state: StreamProfileState::Disabled,
        }
    }

    pub(crate) fn object_id(&self) -> &'static str {
        stream_profile_object_id()
    }

    pub(crate) fn mode(&self) -> StreamProfileMode {
        self.state.mode()
    }

    pub(crate) fn streaming_enabled(&self) -> bool {
        matches!(self.state, StreamProfileState::Enabled { .. })
    }

    pub(crate) fn retired_error(&self) -> Option<OmniError> {
        let StreamProfileState::Retired {
            authority_retirement_id,
            authority_retirement_cut_digest,
            ..
        } = &self.state
        else {
            return None;
        };
        Some(OmniError::StreamAuthorityRetired {
            retirement_id: authority_retirement_id.clone(),
            export_cut_digest: authority_retirement_cut_digest.clone(),
        })
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.profile_revision == 0 {
            return Err(profile_error(
                "stream profile revision must be positive (genesis starts at 1)",
            ));
        }
        self.profile_receipt_chain.validate()?;
        self.state.validate(self.profile_revision)
    }

    pub(crate) fn enabled_from(
        prior: &Self,
        profile_receipt_chain: ReceiptChainRef,
        delegation: FoldDelegation,
    ) -> Result<Self> {
        let next_revision = next_revision(prior.profile_revision)?;
        let next = Self {
            profile_revision: next_revision,
            profile_receipt_chain,
            state: StreamProfileState::Enabled {
                active_fold_delegation: delegation,
            },
        };
        next.validate_transition_from(prior)?;
        Ok(next)
    }

    pub(crate) fn disabling_from(prior: &Self, disable_plan: DisablePlan) -> Result<Self> {
        let next = Self {
            profile_revision: next_revision(prior.profile_revision)?,
            profile_receipt_chain: prior.profile_receipt_chain.clone(),
            state: StreamProfileState::Disabling { disable_plan },
        };
        next.validate_transition_from(prior)?;
        Ok(next)
    }

    pub(crate) fn disabled_from_disabling(
        prior: &Self,
        profile_receipt_chain: ReceiptChainRef,
    ) -> Result<Self> {
        let next = Self {
            profile_revision: next_revision(prior.profile_revision)?,
            profile_receipt_chain,
            state: StreamProfileState::Disabled,
        };
        next.validate_transition_from(prior)?;
        Ok(next)
    }

    #[allow(dead_code)] // Fail-closed vocabulary; the later lifecycle strand activates it.
    pub(crate) fn retired_from_disabled(
        prior: &Self,
        profile_receipt_chain: ReceiptChainRef,
        authority_retirement_id: impl Into<String>,
        authority_retirement_receipt_id: impl Into<String>,
        authority_retirement_cut_digest: impl Into<String>,
    ) -> Result<Self> {
        let next = Self {
            profile_revision: next_revision(prior.profile_revision)?,
            profile_receipt_chain,
            state: StreamProfileState::Retired {
                authority_retirement_id: authority_retirement_id.into(),
                authority_retirement_receipt_id: authority_retirement_receipt_id.into(),
                authority_retirement_cut_digest: authority_retirement_cut_digest.into(),
            },
        };
        next.validate_transition_from(prior)?;
        Ok(next)
    }

    pub(crate) fn validate_transition_from(&self, prior: &Self) -> Result<()> {
        prior.validate()?;
        self.validate()?;
        if self.profile_revision != next_revision(prior.profile_revision)? {
            return Err(profile_error(
                "stream profile transition must advance revision exactly once",
            ));
        }
        match (&prior.state, &self.state) {
            (
                StreamProfileState::Disabled,
                StreamProfileState::Enabled {
                    active_fold_delegation,
                },
            ) => {
                validate_receipt_chain_advance(
                    &prior.profile_receipt_chain,
                    &self.profile_receipt_chain,
                    "profile enable",
                )?;
                if active_fold_delegation.enabled_profile_revision != self.profile_revision {
                    return Err(profile_error(
                        "profile enable delegation revision does not match the next profile",
                    ));
                }
            }
            (
                StreamProfileState::Enabled {
                    active_fold_delegation,
                },
                StreamProfileState::Disabling { disable_plan },
            ) => {
                if self.profile_receipt_chain != prior.profile_receipt_chain {
                    return Err(profile_error(
                        "the first disable publication must not advance the terminal receipt chain",
                    ));
                }
                let continuation = &disable_plan.fold_continuation;
                if continuation.source_delegation_id != active_fold_delegation.delegation_id
                    || continuation.source_delegation_digest
                        != active_fold_delegation.delegation_digest
                    || continuation.cluster_root_digest
                        != active_fold_delegation.cluster_root_digest
                    || continuation.declaration_digest != active_fold_delegation.declaration_digest
                {
                    return Err(profile_error(
                        "disable plan does not consume the exact active fold delegation",
                    ));
                }
            }
            (StreamProfileState::Disabling { .. }, StreamProfileState::Disabled) => {
                validate_receipt_chain_advance(
                    &prior.profile_receipt_chain,
                    &self.profile_receipt_chain,
                    "terminal disable",
                )?;
            }
            (StreamProfileState::Disabled, StreamProfileState::Retired { .. }) => {
                validate_receipt_chain_advance(
                    &prior.profile_receipt_chain,
                    &self.profile_receipt_chain,
                    "authority retirement",
                )?;
            }
            _ => {
                return Err(profile_error(format!(
                    "unsupported stream-profile transition {} -> {}",
                    prior.mode().as_str(),
                    self.mode().as_str()
                )));
            }
        }
        Ok(())
    }

    pub(super) fn to_metadata_json(&self) -> Result<String> {
        self.validate()?;
        serde_json::to_string(&StreamProfilePayload {
            protocol_version: STREAM_PROFILE_PROTOCOL_VERSION,
            profile_revision: self.profile_revision,
            profile_receipt_chain: self.profile_receipt_chain.clone(),
            state: self.state.clone(),
        })
        .map_err(|error| {
            profile_error(format!("failed to encode stream-profile metadata: {error}"))
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
            return Err(profile_error(format!(
                "manifest stream_profile row has object_id '{object_id}', expected '{expected_object_id}'"
            )));
        }
        if location.is_some() || table_version.is_some() || table_branch.is_some() {
            return Err(profile_error(
                "manifest stream_profile row must have null location, table_version, and table_branch",
            ));
        }
        let payload: StreamProfilePayload =
            serde_json::from_str(metadata_json).map_err(|error| {
                profile_error(format!("failed to decode stream-profile metadata: {error}"))
            })?;
        if payload.protocol_version != STREAM_PROFILE_PROTOCOL_VERSION {
            return Err(profile_error(format!(
                "unsupported stream-profile payload version {}, expected {}",
                payload.protocol_version, STREAM_PROFILE_PROTOCOL_VERSION
            )));
        }
        let entry = Self {
            profile_revision: payload.profile_revision,
            profile_receipt_chain: payload.profile_receipt_chain,
            state: payload.state,
        };
        entry.validate()?;
        Ok(entry)
    }
}

/// Bounded result retained by one immutable profile-management receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProfileManagementResult {
    pub(crate) manifest_version: u64,
    pub(crate) profile_revision: u64,
    pub(crate) state: StreamProfileState,
    pub(crate) resume_required_count: u64,
    pub(crate) sealed_identities_digest: String,
}

impl ProfileManagementResult {
    pub(crate) fn new(
        manifest_version: u64,
        profile_revision: u64,
        state: StreamProfileState,
        resume_required_count: u64,
        sealed_identities_digest: impl Into<String>,
    ) -> Result<Self> {
        let value = Self {
            manifest_version,
            profile_revision,
            state,
            resume_required_count,
            sealed_identities_digest: sealed_identities_digest.into(),
        };
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.manifest_version == 0 || self.profile_revision == 0 {
            return Err(profile_error(
                "profile-management result manifest/profile revisions must be positive",
            ));
        }
        self.state.validate(self.profile_revision)?;
        if !matches!(
            self.state,
            StreamProfileState::Disabled | StreamProfileState::Enabled { .. }
        ) {
            return Err(profile_error(
                "profile-management receipts may record only terminal ENABLED or DISABLED results",
            ));
        }
        validate_sha256(
            "profile-management sealed-identities digest",
            &self.sealed_identities_digest,
        )
    }

    fn digest(&self) -> Result<String> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|error| {
            profile_error(format!(
                "failed to encode profile-management result preimage: {error}"
            ))
        })?;
        Ok(hash_fields(PROFILE_RECEIPT_RESULT_DOMAIN, &[&bytes]))
    }
}

/// Immutable, individually bounded receipt row stored in the tagged v2
/// stream-token control ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProfileManagementReceipt {
    pub(crate) protocol_version: u32,
    pub(crate) record_id: String,
    pub(crate) record_lookup_key: String,
    pub(crate) record_tag: String,
    pub(crate) graph_identity_digest: String,
    pub(crate) chain_ordinal: u64,
    pub(crate) predecessor_record_id: Option<String>,
    pub(crate) prior_chain_digest: String,
    pub(crate) resulting_chain_digest: String,
    pub(crate) operation_id: String,
    pub(crate) request_digest: String,
    pub(crate) declaration_revision: String,
    pub(crate) declaration_digest: String,
    pub(crate) actor: String,
    pub(crate) expected_profile_revision: u64,
    pub(crate) result: ProfileManagementResult,
    pub(crate) result_digest: String,
    pub(crate) recorded_at: i64,
}

impl ProfileManagementReceipt {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        graph_identity_digest: impl Into<String>,
        prior_chain: &ReceiptChainRef,
        operation_id: impl Into<String>,
        request_digest: impl Into<String>,
        declaration_revision: impl Into<String>,
        declaration_digest: impl Into<String>,
        actor: impl Into<String>,
        expected_profile_revision: u64,
        result: ProfileManagementResult,
        recorded_at: i64,
    ) -> Result<Self> {
        prior_chain.validate()?;
        let graph_identity_digest = graph_identity_digest.into();
        let operation_id = operation_id.into();
        let mut value = Self {
            protocol_version: PROFILE_MANAGEMENT_RECEIPT_PROTOCOL_VERSION,
            record_id: String::new(),
            record_lookup_key: Self::lookup_key_for(&graph_identity_digest, &operation_id)?,
            record_tag: PROFILE_MANAGEMENT_RECEIPT_TAG.to_string(),
            graph_identity_digest,
            chain_ordinal: prior_chain
                .record_count
                .checked_add(1)
                .ok_or_else(|| profile_error("profile receipt-chain count overflow"))?,
            predecessor_record_id: prior_chain.head_record_id.clone(),
            prior_chain_digest: prior_chain.chain_digest.clone(),
            resulting_chain_digest: String::new(),
            operation_id,
            request_digest: request_digest.into(),
            declaration_revision: declaration_revision.into(),
            declaration_digest: declaration_digest.into(),
            actor: actor.into(),
            expected_profile_revision,
            result,
            result_digest: String::new(),
            recorded_at,
        };
        value.result_digest = value.result.digest()?;
        value.record_id = value.compute_record_id();
        value.resulting_chain_digest = value.compute_resulting_chain_digest();
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn lookup_key_for(
        graph_identity_digest: &str,
        operation_id: &str,
    ) -> Result<String> {
        validate_sha256(
            "profile-management graph identity digest",
            graph_identity_digest,
        )?;
        validate_profile_text("profile-management operation id", operation_id)?;
        Ok(format!(
            "profile-management-v1:{}",
            hash_fields(
                PROFILE_RECEIPT_LOOKUP_DOMAIN,
                &[graph_identity_digest.as_bytes(), operation_id.as_bytes()]
            )
        ))
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.protocol_version != PROFILE_MANAGEMENT_RECEIPT_PROTOCOL_VERSION {
            return Err(profile_error(format!(
                "unsupported profile-management receipt protocol {}, expected {}",
                self.protocol_version, PROFILE_MANAGEMENT_RECEIPT_PROTOCOL_VERSION
            )));
        }
        if self.record_tag != PROFILE_MANAGEMENT_RECEIPT_TAG {
            return Err(profile_error(format!(
                "profile-management receipt tag must be '{PROFILE_MANAGEMENT_RECEIPT_TAG}'"
            )));
        }
        validate_sha256("profile-management record id", &self.record_id)?;
        validate_sha256(
            "profile-management graph identity digest",
            &self.graph_identity_digest,
        )?;
        validate_profile_text("profile-management operation id", &self.operation_id)?;
        validate_sha256("profile-management request digest", &self.request_digest)?;
        validate_profile_text(
            "profile-management declaration revision",
            &self.declaration_revision,
        )?;
        validate_sha256(
            "profile-management declaration digest",
            &self.declaration_digest,
        )?;
        validate_profile_text("profile-management actor", &self.actor)?;
        if self.chain_ordinal == 0 || self.expected_profile_revision == 0 {
            return Err(profile_error(
                "profile-management chain ordinal and expected profile revision must be positive",
            ));
        }
        match (self.chain_ordinal, self.predecessor_record_id.as_deref()) {
            (1, None) => {}
            (1, Some(_)) => {
                return Err(profile_error(
                    "first profile-management receipt cannot have a predecessor",
                ));
            }
            (_, Some(predecessor)) => {
                validate_sha256("profile-management predecessor id", predecessor)?;
            }
            (_, None) => {
                return Err(profile_error(
                    "non-first profile-management receipt requires a predecessor",
                ));
            }
        }
        validate_sha256(
            "profile-management prior chain digest",
            &self.prior_chain_digest,
        )?;
        validate_sha256(
            "profile-management resulting chain digest",
            &self.resulting_chain_digest,
        )?;
        self.result.validate()?;
        if let StreamProfileState::Enabled {
            active_fold_delegation,
        } = &self.result.state
        {
            if active_fold_delegation.issuing_actor != self.actor
                || active_fold_delegation.declaration_revision != self.declaration_revision
                || active_fold_delegation.declaration_digest != self.declaration_digest
            {
                return Err(profile_error(
                    "profile-management ENABLED result delegation differs from the receipt actor or declaration",
                ));
            }
        }
        if self.result.profile_revision
            != self
                .expected_profile_revision
                .checked_add(1)
                .ok_or_else(|| profile_error("profile-management revision overflow"))?
        {
            return Err(profile_error(
                "profile-management result must advance the expected revision exactly once",
            ));
        }
        let expected_request_digest = stream_profile_management_request_digest(
            &self.graph_identity_digest,
            &self.operation_id,
            &self.declaration_revision,
            &self.declaration_digest,
            self.expected_profile_revision,
            self.result.state.mode(),
        )?;
        if self.request_digest != expected_request_digest {
            return Err(profile_error(
                "profile-management receipt request digest differs from its canonical request preimage",
            ));
        }
        validate_sha256("profile-management result digest", &self.result_digest)?;
        if self.recorded_at <= 0 {
            return Err(profile_error(
                "profile-management recorded_at must be a positive timestamp",
            ));
        }
        if self.record_lookup_key
            != Self::lookup_key_for(&self.graph_identity_digest, &self.operation_id)?
            || self.result_digest != self.result.digest()?
            || self.record_id != self.compute_record_id()
            || self.resulting_chain_digest != self.compute_resulting_chain_digest()
        {
            return Err(profile_error(
                "profile-management receipt differs from its canonical identity or chain commitment",
            ));
        }
        Ok(())
    }

    pub(crate) fn next_chain_ref(&self) -> Result<ReceiptChainRef> {
        self.validate()?;
        Ok(ReceiptChainRef {
            head_record_id: Some(self.record_id.clone()),
            record_count: self.chain_ordinal,
            chain_digest: self.resulting_chain_digest.clone(),
        })
    }

    pub(crate) fn validate_terminal_disable_plan_binding(
        &self,
        disable_plan: &DisablePlan,
    ) -> Result<()> {
        self.validate()?;
        disable_plan.validate()?;
        if !matches!(self.result.state, StreamProfileState::Disabled)
            || self.operation_id != disable_plan.operation_id
            || self.request_digest != disable_plan.request_digest
            || self.actor != disable_plan.actor
            || self.declaration_revision != disable_plan.declaration_revision
            || self.declaration_digest != disable_plan.declaration_digest
        {
            return Err(profile_error(
                "terminal disable receipt does not bind the exact durable DisablePlan",
            ));
        }
        Ok(())
    }

    fn compute_record_id(&self) -> String {
        let predecessor = self.predecessor_record_id.as_deref().unwrap_or("");
        hash_fields(
            PROFILE_RECEIPT_RECORD_DOMAIN,
            &[
                self.record_tag.as_bytes(),
                self.graph_identity_digest.as_bytes(),
                &self.chain_ordinal.to_be_bytes(),
                predecessor.as_bytes(),
                self.prior_chain_digest.as_bytes(),
                self.operation_id.as_bytes(),
                self.request_digest.as_bytes(),
                self.declaration_revision.as_bytes(),
                self.declaration_digest.as_bytes(),
                self.actor.as_bytes(),
                &self.expected_profile_revision.to_be_bytes(),
                self.result_digest.as_bytes(),
                &self.recorded_at.to_be_bytes(),
            ],
        )
    }

    fn compute_resulting_chain_digest(&self) -> String {
        hash_fields(
            PROFILE_RECEIPT_CHAIN_DOMAIN,
            &[
                self.prior_chain_digest.as_bytes(),
                &self.chain_ordinal.to_be_bytes(),
                self.record_id.as_bytes(),
            ],
        )
    }
}

/// Immutable proof that one exact terminal stream-authority cut was retired.
///
/// The receipt advances the existing graph-global profile receipt chain. It is
/// provenance for rebuilding logical rows into a fresh graph identity, never
/// live sequencing authority in the rebuilt graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AuthorityRetirementReceipt {
    pub(crate) protocol_version: u32,
    pub(crate) record_id: String,
    pub(crate) record_lookup_key: String,
    pub(crate) record_tag: String,
    pub(crate) graph_identity_digest: String,
    pub(crate) chain_ordinal: u64,
    pub(crate) predecessor_record_id: Option<String>,
    pub(crate) prior_chain_digest: String,
    pub(crate) resulting_chain_digest: String,
    pub(crate) retirement_id: String,
    pub(crate) plan_digest: String,
    pub(crate) operation_request_digest: String,
    pub(crate) actor: String,
    pub(crate) source_internal_schema_version: u32,
    pub(crate) source_manifest_version: u64,
    pub(crate) live_branch_heads_digest: String,
    pub(crate) source_profile_revision: u64,
    pub(crate) lifecycle_and_sealed_proof_digest: String,
    pub(crate) pre_retirement_token_head: super::CurrentHeadWitness,
    pub(crate) pre_retirement_token_witness_digest: String,
    pub(crate) present_token_count: u64,
    pub(crate) withdrawn_token_count: u64,
    pub(crate) export_cut_digest: String,
    pub(crate) retired_at: i64,
}

impl AuthorityRetirementReceipt {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        graph_identity_digest: impl Into<String>,
        prior_chain: &ReceiptChainRef,
        retirement_id: impl Into<String>,
        plan_digest: impl Into<String>,
        actor: impl Into<String>,
        source_internal_schema_version: u32,
        source_manifest_version: u64,
        live_branch_heads_digest: impl Into<String>,
        source_profile_revision: u64,
        lifecycle_and_sealed_proof_digest: impl Into<String>,
        pre_retirement_token_head: super::CurrentHeadWitness,
        pre_retirement_token_witness_digest: impl Into<String>,
        present_token_count: u64,
        withdrawn_token_count: u64,
        export_cut_digest: impl Into<String>,
        retired_at: i64,
    ) -> Result<Self> {
        prior_chain.validate()?;
        let graph_identity_digest = graph_identity_digest.into();
        let retirement_id = retirement_id.into();
        let plan_digest = plan_digest.into();
        let actor = actor.into();
        let mut value = Self {
            protocol_version: AUTHORITY_RETIREMENT_RECEIPT_PROTOCOL_VERSION,
            record_id: String::new(),
            record_lookup_key: Self::lookup_key_for(&graph_identity_digest, &retirement_id)?,
            record_tag: AUTHORITY_RETIREMENT_RECEIPT_TAG.to_string(),
            graph_identity_digest,
            chain_ordinal: prior_chain
                .record_count
                .checked_add(1)
                .ok_or_else(|| profile_error("profile receipt-chain count overflow"))?,
            predecessor_record_id: prior_chain.head_record_id.clone(),
            prior_chain_digest: prior_chain.chain_digest.clone(),
            resulting_chain_digest: String::new(),
            retirement_id,
            operation_request_digest: stream_authority_retirement_request_digest(
                &plan_digest,
                &actor,
            )?,
            plan_digest,
            actor,
            source_internal_schema_version,
            source_manifest_version,
            live_branch_heads_digest: live_branch_heads_digest.into(),
            source_profile_revision,
            lifecycle_and_sealed_proof_digest: lifecycle_and_sealed_proof_digest.into(),
            pre_retirement_token_head,
            pre_retirement_token_witness_digest: pre_retirement_token_witness_digest.into(),
            present_token_count,
            withdrawn_token_count,
            export_cut_digest: export_cut_digest.into(),
            retired_at,
        };
        value.record_id = value.compute_record_id();
        value.resulting_chain_digest = value.compute_resulting_chain_digest();
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn lookup_key_for(
        graph_identity_digest: &str,
        retirement_id: &str,
    ) -> Result<String> {
        validate_sha256(
            "authority-retirement graph identity digest",
            graph_identity_digest,
        )?;
        validate_canonical_uuid("authority-retirement id", retirement_id)?;
        Ok(format!(
            "authority-retirement-v1:{}",
            hash_fields(
                AUTHORITY_RETIREMENT_RECEIPT_LOOKUP_DOMAIN,
                &[graph_identity_digest.as_bytes(), retirement_id.as_bytes()],
            )
        ))
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.protocol_version != AUTHORITY_RETIREMENT_RECEIPT_PROTOCOL_VERSION {
            return Err(profile_error(format!(
                "unsupported authority-retirement receipt protocol {}, expected {}",
                self.protocol_version, AUTHORITY_RETIREMENT_RECEIPT_PROTOCOL_VERSION
            )));
        }
        if self.record_tag != AUTHORITY_RETIREMENT_RECEIPT_TAG {
            return Err(profile_error(format!(
                "authority-retirement receipt tag must be '{AUTHORITY_RETIREMENT_RECEIPT_TAG}'"
            )));
        }
        validate_sha256("authority-retirement record id", &self.record_id)?;
        validate_sha256(
            "authority-retirement graph identity digest",
            &self.graph_identity_digest,
        )?;
        validate_canonical_uuid("authority-retirement id", &self.retirement_id)?;
        validate_sha256("authority-retirement plan digest", &self.plan_digest)?;
        validate_sha256(
            "authority-retirement operation request digest",
            &self.operation_request_digest,
        )?;
        validate_profile_text("authority-retirement actor", &self.actor)?;
        if self.source_internal_schema_version == 0
            || self.source_manifest_version == 0
            || self.source_profile_revision == 0
            || self.chain_ordinal == 0
        {
            return Err(profile_error(
                "authority-retirement format, manifest, profile, and chain revisions must be positive",
            ));
        }
        if self.withdrawn_token_count == 0 {
            return Err(profile_error(
                "authority retirement requires at least one current WITHDRAWN token",
            ));
        }
        self.pre_retirement_token_head.validate()?;
        if self.pre_retirement_token_head.branch_identifier
            != lance::dataset::refs::BranchIdentifier::main()
            || self.pre_retirement_token_head.manifest_e_tag.is_some()
            || self.pre_retirement_token_witness_digest
                != stream_authority_retirement_token_witness_digest(
                    &self.pre_retirement_token_head,
                    self.present_token_count,
                    self.withdrawn_token_count,
                )?
        {
            return Err(profile_error(
                "authority-retirement token proof does not bind the exact canonical prior head and disposition counts",
            ));
        }
        for (name, digest) in [
            ("live branch-heads", &self.live_branch_heads_digest),
            (
                "lifecycle and SEALED proof",
                &self.lifecycle_and_sealed_proof_digest,
            ),
            (
                "pre-retirement token witness",
                &self.pre_retirement_token_witness_digest,
            ),
            ("export cut", &self.export_cut_digest),
            ("prior chain", &self.prior_chain_digest),
            ("resulting chain", &self.resulting_chain_digest),
        ] {
            validate_sha256(&format!("authority-retirement {name} digest"), digest)?;
        }
        match (self.chain_ordinal, self.predecessor_record_id.as_deref()) {
            (1, None) => {}
            (1, Some(_)) => {
                return Err(profile_error(
                    "first authority-retirement receipt cannot have a predecessor",
                ));
            }
            (_, Some(predecessor)) => {
                validate_sha256("authority-retirement predecessor id", predecessor)?;
            }
            (_, None) => {
                return Err(profile_error(
                    "non-first authority-retirement receipt requires a predecessor",
                ));
            }
        }
        if self.retired_at <= 0 {
            return Err(profile_error(
                "authority-retirement retired_at must be a positive timestamp",
            ));
        }
        if self.record_lookup_key
            != Self::lookup_key_for(&self.graph_identity_digest, &self.retirement_id)?
            || self.operation_request_digest
                != stream_authority_retirement_request_digest(&self.plan_digest, &self.actor)?
            || self.record_id != self.compute_record_id()
            || self.resulting_chain_digest != self.compute_resulting_chain_digest()
        {
            return Err(profile_error(
                "authority-retirement receipt differs from its canonical identity or chain commitment",
            ));
        }
        Ok(())
    }

    pub(crate) fn next_chain_ref(&self) -> Result<ReceiptChainRef> {
        self.validate()?;
        Ok(ReceiptChainRef {
            head_record_id: Some(self.record_id.clone()),
            record_count: self.chain_ordinal,
            chain_digest: self.resulting_chain_digest.clone(),
        })
    }

    fn compute_record_id(&self) -> String {
        let predecessor = self.predecessor_record_id.as_deref().unwrap_or("");
        hash_fields(
            AUTHORITY_RETIREMENT_RECEIPT_RECORD_DOMAIN,
            &[
                self.record_tag.as_bytes(),
                self.graph_identity_digest.as_bytes(),
                &self.chain_ordinal.to_be_bytes(),
                predecessor.as_bytes(),
                self.prior_chain_digest.as_bytes(),
                self.retirement_id.as_bytes(),
                self.plan_digest.as_bytes(),
                self.operation_request_digest.as_bytes(),
                self.actor.as_bytes(),
                &self.source_internal_schema_version.to_be_bytes(),
                &self.source_manifest_version.to_be_bytes(),
                self.live_branch_heads_digest.as_bytes(),
                &self.source_profile_revision.to_be_bytes(),
                self.lifecycle_and_sealed_proof_digest.as_bytes(),
                &self.pre_retirement_token_head.table_version.to_be_bytes(),
                self.pre_retirement_token_head.transaction_uuid.as_bytes(),
                self.pre_retirement_token_witness_digest.as_bytes(),
                &self.present_token_count.to_be_bytes(),
                &self.withdrawn_token_count.to_be_bytes(),
                self.export_cut_digest.as_bytes(),
                &self.retired_at.to_be_bytes(),
            ],
        )
    }

    fn compute_resulting_chain_digest(&self) -> String {
        hash_fields(
            AUTHORITY_RETIREMENT_RECEIPT_CHAIN_DOMAIN,
            &[
                self.prior_chain_digest.as_bytes(),
                &self.chain_ordinal.to_be_bytes(),
                self.record_id.as_bytes(),
            ],
        )
    }
}

pub(crate) fn stream_authority_retirement_request_digest(
    plan_digest: &str,
    actor: &str,
) -> Result<String> {
    validate_sha256("authority-retirement plan digest", plan_digest)?;
    validate_profile_text("authority-retirement actor", actor)?;
    Ok(hash_fields(
        AUTHORITY_RETIREMENT_REQUEST_DOMAIN,
        &[
            &AUTHORITY_RETIREMENT_RECEIPT_PROTOCOL_VERSION.to_be_bytes(),
            plan_digest.as_bytes(),
            actor.as_bytes(),
            b"CONFIRM",
        ],
    ))
}

pub(crate) fn stream_authority_retirement_token_witness_digest(
    witness: &super::CurrentHeadWitness,
    present_token_count: u64,
    withdrawn_token_count: u64,
) -> Result<String> {
    witness.validate()?;
    if witness.manifest_e_tag.is_some() {
        return Err(profile_error(
            "authority-retirement token witness must carry e-tag None",
        ));
    }
    let witness_bytes = serde_json::to_vec(witness).map_err(|error| {
        profile_error(format!(
            "failed to encode authority-retirement token witness: {error}"
        ))
    })?;
    Ok(hash_fields(
        AUTHORITY_RETIREMENT_TOKEN_WITNESS_DOMAIN,
        &[
            &witness_bytes,
            &present_token_count.to_be_bytes(),
            &withdrawn_token_count.to_be_bytes(),
        ],
    ))
}

pub(crate) fn stream_profile_graph_identity_digest(
    cluster_root_digest: &str,
    graph_id: &str,
    graph_store_uri: &str,
) -> Result<String> {
    validate_sha256("cluster-root digest", cluster_root_digest)?;
    validate_profile_text("graph id", graph_id)?;
    validate_profile_text("graph store URI", graph_store_uri)?;
    Ok(hash_fields(
        b"omnigraph.stream-profile-graph-identity.v1\0",
        &[
            cluster_root_digest.as_bytes(),
            graph_id.as_bytes(),
            graph_store_uri.as_bytes(),
        ],
    ))
}

pub(crate) fn stream_profile_management_request_digest(
    graph_identity_digest: &str,
    operation_id: &str,
    declaration_revision: &str,
    declaration_digest: &str,
    expected_profile_revision: u64,
    desired_mode: StreamProfileMode,
) -> Result<String> {
    validate_sha256("graph identity digest", graph_identity_digest)?;
    validate_profile_text("profile-management operation id", operation_id)?;
    validate_profile_text(
        "profile-management declaration revision",
        declaration_revision,
    )?;
    validate_sha256("profile-management declaration digest", declaration_digest)?;
    if expected_profile_revision == 0 {
        return Err(profile_error(
            "profile-management expected revision must be positive",
        ));
    }
    Ok(hash_fields(
        PROFILE_REQUEST_DOMAIN,
        &[
            graph_identity_digest.as_bytes(),
            operation_id.as_bytes(),
            declaration_revision.as_bytes(),
            declaration_digest.as_bytes(),
            &expected_profile_revision.to_be_bytes(),
            desired_mode.as_str().as_bytes(),
        ],
    ))
}

/// Public projection computed from one already-loaded manifest snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamingStatus {
    pub enabled: bool,
    pub undrained: bool,
    pub profile_revision: u64,
    pub profile_mode: &'static str,
}

pub(crate) fn stream_profile_cluster_root_digest(cluster_root: &str) -> Result<String> {
    validate_profile_text("cluster root", cluster_root)?;
    Ok(hash_fields(
        b"omnigraph.stream-profile-cluster-root.v1\0",
        &[cluster_root.as_bytes()],
    ))
}

fn next_revision(revision: u64) -> Result<u64> {
    revision
        .checked_add(1)
        .ok_or_else(|| profile_error("stream profile revision overflow"))
}

fn validate_receipt_chain_advance(
    prior: &ReceiptChainRef,
    next: &ReceiptChainRef,
    transition: &str,
) -> Result<()> {
    let expected_count = prior
        .record_count
        .checked_add(1)
        .ok_or_else(|| profile_error("profile receipt-chain count overflow"))?;
    if next.record_count != expected_count
        || next.head_record_id == prior.head_record_id
        || next.chain_digest == prior.chain_digest
    {
        return Err(profile_error(format!(
            "{transition} must advance its management-receipt chain by exactly one new record"
        )));
    }
    Ok(())
}

fn validate_profile_text(name: &str, value: &str) -> Result<()> {
    if value.trim().is_empty()
        || value.trim() != value
        || value.len() > MAX_PROFILE_TEXT_BYTES
        || value.as_bytes().contains(&0)
    {
        return Err(profile_error(format!(
            "{name} must be non-empty canonical text no larger than {MAX_PROFILE_TEXT_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_sha256(name: &str, value: &str) -> Result<()> {
    let Some(hex) = value.strip_prefix(SHA256_PREFIX) else {
        return Err(profile_error(format!(
            "{name} must use sha256:<lowerhex> encoding"
        )));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(profile_error(format!(
            "{name} must contain exactly 64 lowercase hexadecimal digits"
        )));
    }
    Ok(())
}

fn validate_canonical_uuid(name: &str, value: &str) -> Result<()> {
    let parsed = ShardId::parse_str(value)
        .map_err(|error| profile_error(format!("{name} is not a UUID: {error}")))?;
    if parsed.is_nil() || parsed.to_string() != value {
        return Err(profile_error(format!(
            "{name} must be a non-nil canonical lowercase hyphenated UUID"
        )));
    }
    Ok(())
}

fn hash_fields(domain: &[u8], fields: &[&[u8]]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    for field in fields {
        hasher.update((field.len() as u64).to_be_bytes());
        hasher.update(field);
    }
    format!("{SHA256_PREFIX}{:x}", hasher.finalize())
}

fn sha256(bytes: &[u8]) -> String {
    format!("{SHA256_PREFIX}{:x}", Sha256::digest(bytes))
}

fn profile_error(message: impl Into<String>) -> OmniError {
    OmniError::manifest_internal(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    const DIGEST_A: &str =
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const DIGEST_B: &str =
        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn advanced_chain(prior: &ReceiptChainRef) -> ReceiptChainRef {
        let next_count = prior.record_count + 1;
        ReceiptChainRef {
            head_record_id: Some(sha256(format!("test-head-{next_count}").as_bytes())),
            record_count: next_count,
            chain_digest: sha256(format!("test-chain-{next_count}").as_bytes()),
        }
    }

    fn delegation(revision: u64) -> FoldDelegation {
        FoldDelegation::issue(
            "11111111-1111-4111-8111-111111111111",
            DIGEST_A,
            "config-revision-7",
            DIGEST_B,
            revision,
            "operator:alice",
            1_700_000_000_000_000,
        )
        .unwrap()
    }

    fn reseal_profile_receipt(receipt: &mut ProfileManagementReceipt) {
        if let StreamProfileState::Enabled {
            active_fold_delegation,
        } = &mut receipt.result.state
        {
            active_fold_delegation.delegation_digest = active_fold_delegation.compute_digest();
        }
        receipt.result_digest = receipt.result.digest().unwrap();
        receipt.record_id = receipt.compute_record_id();
        receipt.resulting_chain_digest = receipt.compute_resulting_chain_digest();
    }

    #[test]
    fn v11_genesis_round_trips_with_exact_protocol_gate() {
        let genesis = StreamProfileEntry::genesis();
        assert_eq!(genesis.mode(), StreamProfileMode::Disabled);
        assert_eq!(genesis.profile_revision, 1);
        assert_eq!(genesis.profile_receipt_chain, ReceiptChainRef::genesis());

        let json = genesis.to_metadata_json().unwrap();
        assert!(json.contains("\"protocol_version\":2"), "{json}");
        assert!(json.contains("\"mode\":\"DISABLED\""), "{json}");
        assert!(!json.contains("streaming_enabled"), "{json}");
        let decoded = StreamProfileEntry::from_manifest_row(
            stream_profile_object_id(),
            None,
            None,
            None,
            &json,
        )
        .unwrap();
        assert_eq!(decoded, genesis);
    }

    #[test]
    fn enable_and_disable_consume_only_the_exact_delegation() {
        let genesis = StreamProfileEntry::genesis();
        let jumped_chain = ReceiptChainRef {
            head_record_id: Some(DIGEST_A.to_string()),
            record_count: 2,
            chain_digest: DIGEST_B.to_string(),
        };
        let error = StreamProfileEntry::enabled_from(&genesis, jumped_chain, delegation(2))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("exactly one new record"),
            "receipt ordinals must remain contiguous: {error}"
        );
        let enabled = StreamProfileEntry::enabled_from(
            &genesis,
            advanced_chain(&genesis.profile_receipt_chain),
            delegation(2),
        )
        .unwrap();
        let active = match &enabled.state {
            StreamProfileState::Enabled {
                active_fold_delegation,
            } => active_fold_delegation,
            other => panic!("unexpected state {other:?}"),
        };
        let continuation = FoldContinuation::derive(active, 3).unwrap();
        for invalid_id in [
            "00000000-0000-0000-0000-000000000000",
            "11111111-1111-4111-8111-11111111111A",
        ] {
            let mut invalid = continuation.clone();
            invalid.source_delegation_id = invalid_id.to_string();
            invalid.continuation_digest = invalid.compute_digest();
            let error = invalid.validate().unwrap_err().to_string();
            assert!(
                error.contains("non-nil canonical lowercase hyphenated UUID"),
                "self-consistent continuation must reject non-canonical source delegation id '{invalid_id}': {error}"
            );
        }
        let target_disabled_declaration = format!("sha256:{}", "c".repeat(64));
        let mut wrong_declaration = continuation.clone();
        wrong_declaration.declaration_digest = DIGEST_A.to_string();
        wrong_declaration.continuation_digest = wrong_declaration.compute_digest();
        let wrong_plan = DisablePlan::new(
            "disable-wrong-declaration",
            DIGEST_A,
            "config-revision-disabled-8",
            &target_disabled_declaration,
            "operator:alice",
            wrong_declaration,
        )
        .unwrap();
        let error = StreamProfileEntry::disabling_from(&enabled, wrong_plan)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("exact active fold delegation"),
            "self-consistent continuation must retain the source delegation declaration digest: {error}"
        );
        let plan = DisablePlan::new(
            "disable-1",
            DIGEST_A,
            "config-revision-disabled-8",
            &target_disabled_declaration,
            "operator:alice",
            continuation,
        )
        .unwrap();
        assert_eq!(plan.declaration_revision, "config-revision-disabled-8");
        assert_ne!(
            target_disabled_declaration, active.declaration_digest,
            "the desired disabled declaration normally differs from the consumed enabled declaration"
        );
        let disabling = StreamProfileEntry::disabling_from(&enabled, plan).unwrap();
        assert_eq!(disabling.mode(), StreamProfileMode::Disabling);
        assert_eq!(
            disabling.profile_receipt_chain,
            enabled.profile_receipt_chain
        );
        let disabling_json = disabling.to_metadata_json().unwrap();
        assert!(
            disabling_json.contains("\"declaration_revision\":\"config-revision-disabled-8\""),
            "{disabling_json}"
        );
        assert_eq!(
            StreamProfileEntry::from_manifest_row(
                stream_profile_object_id(),
                None,
                None,
                None,
                &disabling_json,
            )
            .unwrap(),
            disabling
        );
        let disabled = StreamProfileEntry::disabled_from_disabling(
            &disabling,
            advanced_chain(&disabling.profile_receipt_chain),
        )
        .unwrap();
        assert_eq!(disabled.mode(), StreamProfileMode::Disabled);
    }

    #[test]
    fn profile_receipt_binds_bounded_result_and_advances_chain_once() {
        let genesis = StreamProfileEntry::genesis();
        let active = delegation(2);
        let result = ProfileManagementResult::new(
            2,
            2,
            StreamProfileState::Enabled {
                active_fold_delegation: active.clone(),
            },
            0,
            DIGEST_A,
        )
        .unwrap();
        let request_digest = stream_profile_management_request_digest(
            DIGEST_B,
            "enable-1",
            "config-revision-7",
            DIGEST_B,
            1,
            StreamProfileMode::Enabled,
        )
        .unwrap();
        let receipt = ProfileManagementReceipt::new(
            DIGEST_B,
            &genesis.profile_receipt_chain,
            "enable-1",
            request_digest,
            "config-revision-7",
            DIGEST_B,
            "operator:alice",
            1,
            result,
            1_700_000_000_000_001,
        )
        .unwrap();
        let next_chain = receipt.next_chain_ref().unwrap();
        assert_eq!(next_chain.record_count, 1);
        assert_eq!(
            next_chain.head_record_id.as_deref(),
            Some(receipt.record_id.as_str())
        );
        let enabled = StreamProfileEntry::enabled_from(&genesis, next_chain, active).unwrap();
        assert_eq!(enabled.profile_revision, 2);

        let json = serde_json::to_string(&receipt).unwrap();
        assert_eq!(
            serde_json::from_str::<ProfileManagementReceipt>(&json).unwrap(),
            receipt
        );

        let mut tampered = receipt.clone();
        tampered.request_digest = DIGEST_A.to_string();
        tampered.record_id = tampered.compute_record_id();
        tampered.resulting_chain_digest = tampered.compute_resulting_chain_digest();
        let error = tampered.validate().unwrap_err().to_string();
        assert!(
            error.contains("canonical request preimage"),
            "self-consistent receipt commitments must not authenticate a non-canonical request digest: {error}"
        );

        let mutations: [(&str, fn(&mut FoldDelegation)); 3] = [
            ("issuing_actor", |delegation| {
                delegation.issuing_actor = "operator:bob".to_string();
            }),
            ("declaration_revision", |delegation| {
                delegation.declaration_revision = "config-revision-other".to_string();
            }),
            ("declaration_digest", |delegation| {
                delegation.declaration_digest = DIGEST_A.to_string();
            }),
        ];
        for (field, mutate) in mutations {
            let mut tampered = receipt.clone();
            let StreamProfileState::Enabled {
                active_fold_delegation,
            } = &mut tampered.result.state
            else {
                unreachable!("test receipt is ENABLED");
            };
            mutate(active_fold_delegation);
            reseal_profile_receipt(&mut tampered);
            let error = tampered.validate().unwrap_err().to_string();
            assert!(
                error.contains("result delegation differs"),
                "self-consistent ENABLED receipt must bind delegation {field}: {error}"
            );
        }
    }

    #[test]
    fn terminal_disable_receipt_binds_the_exact_durable_plan() {
        let genesis = StreamProfileEntry::genesis();
        let enabled = StreamProfileEntry::enabled_from(
            &genesis,
            advanced_chain(&genesis.profile_receipt_chain),
            delegation(2),
        )
        .unwrap();
        let active = match &enabled.state {
            StreamProfileState::Enabled {
                active_fold_delegation,
            } => active_fold_delegation,
            other => panic!("unexpected state {other:?}"),
        };
        let operation_id = "disable-terminal";
        let declaration_revision = "config-revision-disabled";
        let declaration_digest = DIGEST_A;
        let actor = "operator:alice";
        let request_digest = stream_profile_management_request_digest(
            DIGEST_B,
            operation_id,
            declaration_revision,
            declaration_digest,
            3,
            StreamProfileMode::Disabled,
        )
        .unwrap();
        let plan = DisablePlan::new(
            operation_id,
            &request_digest,
            declaration_revision,
            declaration_digest,
            actor,
            FoldContinuation::derive(active, 3).unwrap(),
        )
        .unwrap();
        let disabling = StreamProfileEntry::disabling_from(&enabled, plan.clone()).unwrap();
        let result =
            ProfileManagementResult::new(4, 4, StreamProfileState::Disabled, 0, DIGEST_A).unwrap();
        let make_receipt = |operation_id: &str,
                            declaration_revision: &str,
                            declaration_digest: &str,
                            actor: &str| {
            let request_digest = stream_profile_management_request_digest(
                DIGEST_B,
                operation_id,
                declaration_revision,
                declaration_digest,
                disabling.profile_revision,
                StreamProfileMode::Disabled,
            )
            .unwrap();
            ProfileManagementReceipt::new(
                DIGEST_B,
                &disabling.profile_receipt_chain,
                operation_id,
                request_digest,
                declaration_revision,
                declaration_digest,
                actor,
                disabling.profile_revision,
                result.clone(),
                1_700_000_000_000_002,
            )
            .unwrap()
        };
        let exact = make_receipt(
            operation_id,
            declaration_revision,
            declaration_digest,
            actor,
        );
        exact.validate_terminal_disable_plan_binding(&plan).unwrap();

        let mismatches = [
            (
                "operation",
                make_receipt(
                    "disable-terminal-other",
                    declaration_revision,
                    declaration_digest,
                    actor,
                ),
            ),
            (
                "actor",
                make_receipt(
                    operation_id,
                    declaration_revision,
                    declaration_digest,
                    "operator:bob",
                ),
            ),
            (
                "declaration revision",
                make_receipt(
                    operation_id,
                    "config-revision-disabled-other",
                    declaration_digest,
                    actor,
                ),
            ),
            (
                "declaration digest",
                make_receipt(operation_id, declaration_revision, DIGEST_B, actor),
            ),
        ];
        for (field, receipt) in mismatches {
            receipt.validate().unwrap();
            let error = receipt
                .validate_terminal_disable_plan_binding(&plan)
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("exact durable DisablePlan"),
                "self-consistent terminal receipt must bind {field}: {error}"
            );
        }

        let mut wrong_request_plan = plan;
        wrong_request_plan.request_digest = DIGEST_A.to_string();
        wrong_request_plan.validate().unwrap();
        let error = exact
            .validate_terminal_disable_plan_binding(&wrong_request_plan)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("exact durable DisablePlan"),
            "terminal receipt must bind the plan's request digest: {error}"
        );
    }

    #[test]
    fn retired_is_terminal_and_requires_exact_payload() {
        let genesis = StreamProfileEntry::genesis();
        let retired = StreamProfileEntry::retired_from_disabled(
            &genesis,
            advanced_chain(&genesis.profile_receipt_chain),
            "19191919-1919-4919-8919-191919191919",
            DIGEST_B,
            DIGEST_A,
        )
        .unwrap();
        let error = StreamProfileEntry::enabled_from(
            &retired,
            advanced_chain(&retired.profile_receipt_chain),
            delegation(3),
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("unsupported stream-profile transition"),
            "{error}"
        );

        let missing = format!(
            r#"{{"protocol_version":2,"profile_revision":2,"profile_receipt_chain":{{"head_record_id":"{DIGEST_A}","record_count":1,"chain_digest":"{DIGEST_B}"}},"state":{{"mode":"RETIRED","authority_retirement_receipt_id":"retirement-1"}}}}"#
        );
        assert!(
            StreamProfileEntry::from_manifest_row(
                stream_profile_object_id(),
                None,
                None,
                None,
                &missing
            )
            .is_err()
        );
    }

    #[test]
    fn authority_retirement_receipt_is_exact_chained_provenance() {
        let prior = StreamProfileEntry::genesis();
        let token_head = crate::db::manifest::CurrentHeadWitness {
            branch_identifier: lance::dataset::refs::BranchIdentifier::main(),
            table_version: 4,
            transaction_uuid: "18181818-1818-4818-8818-181818181818".to_string(),
            manifest_e_tag: None,
        };
        let token_digest =
            stream_authority_retirement_token_witness_digest(&token_head, 7, 2).unwrap();
        let receipt = AuthorityRetirementReceipt::new(
            DIGEST_A,
            &prior.profile_receipt_chain,
            "19191919-1919-4919-8919-191919191919",
            DIGEST_B,
            "operator:alice",
            17,
            9,
            DIGEST_A,
            prior.profile_revision,
            DIGEST_B,
            token_head,
            token_digest,
            7,
            2,
            DIGEST_B,
            1_700_000_000_000_000,
        )
        .unwrap();
        receipt.validate().unwrap();
        let next = StreamProfileEntry::retired_from_disabled(
            &prior,
            receipt.next_chain_ref().unwrap(),
            receipt.retirement_id.clone(),
            receipt.record_id.clone(),
            receipt.export_cut_digest.clone(),
        )
        .unwrap();
        assert_eq!(
            next.retired_error().unwrap().to_string(),
            format!(
                "stream authority retirement '{}' fixed this graph at export cut {}; the source is read/export-only",
                receipt.retirement_id, receipt.export_cut_digest
            )
        );

        let mut unknown = serde_json::to_value(&receipt).unwrap();
        unknown
            .as_object_mut()
            .unwrap()
            .insert("future".to_string(), serde_json::json!(true));
        assert!(
            serde_json::from_value::<AuthorityRetirementReceipt>(unknown).is_err(),
            "retirement provenance must deny unknown fields"
        );

        let mut forged = receipt.clone();
        forged.withdrawn_token_count += 1;
        assert!(forged.validate().is_err());
    }

    #[test]
    fn authority_retirement_token_witness_digest_binds_counts() {
        let witness = super::super::CurrentHeadWitness {
            branch_identifier: lance::dataset::refs::BranchIdentifier::main(),
            table_version: 7,
            transaction_uuid: "17171717-1717-4717-8717-171717171717".to_string(),
            manifest_e_tag: None,
        };
        let first = stream_authority_retirement_token_witness_digest(&witness, 2, 1).unwrap();
        let second = stream_authority_retirement_token_witness_digest(&witness, 2, 2).unwrap();
        assert_ne!(first, second);
    }

    #[test]
    fn v10_boolean_payload_and_unknown_v11_keys_are_refused() {
        let v10 = r#"{"protocol_version":1,"streaming_enabled":false,"disable_pending_since":null,"profile_revision":1}"#;
        let error = StreamProfileEntry::from_manifest_row(
            stream_profile_object_id(),
            None,
            None,
            None,
            v10,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("failed to decode"), "{error}");

        let mut value = serde_json::to_value(StreamProfilePayload {
            protocol_version: 2,
            profile_revision: 1,
            profile_receipt_chain: ReceiptChainRef::genesis(),
            state: StreamProfileState::Disabled,
        })
        .unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("future".to_string(), serde_json::json!(true));
        assert!(
            StreamProfileEntry::from_manifest_row(
                stream_profile_object_id(),
                None,
                None,
                None,
                &serde_json::to_string(&value).unwrap()
            )
            .is_err()
        );
    }
}
