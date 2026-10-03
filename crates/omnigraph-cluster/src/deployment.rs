//! Durable offline schema/query deployments in the existing cluster ledger.
//!
//! Graph publication remains engine authority. This module stores immutable
//! input and the engine's exact outcomes; it never reconstructs a receipt from
//! schema-text equality or replays an interrupted schema invocation.

use super::*;
use omnigraph::db::{PreparedSchemaApply, PreparedSchemaSettlement, SchemaApplySettlement};

mod execution;
pub use execution::*;

pub(crate) const MAX_LEDGER_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_BUNDLE_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_RESULT_BYTES: usize = 1024 * 1024;
pub(crate) const MAX_RESULTS_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const MAX_RESULTS: usize = 32;
pub(crate) const MAX_RESOURCES: usize = 4096;
pub(crate) const MAX_DIAGNOSTIC_BYTES: usize = 4096;
pub(crate) const GRAPH_COMPLETION_RESERVE_BYTES: usize = 8192;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DeploymentMode {
    Offline,
}

/// The caller still owns authentication. A stored authority record never
/// grants permission to invoke an effect or disclose an earlier result.
#[derive(Debug, Clone)]
pub enum DeploymentCaller {
    StorageOwner { actor: Option<String> },
    AuthenticatedIdentity(IdentityAuthorization),
}

impl DeploymentCaller {
    pub fn storage_owner(actor: Option<String>) -> Self {
        Self::StorageOwner { actor }
    }

    pub fn actor(&self) -> Option<&str> {
        match self {
            Self::StorageOwner { actor } => actor.as_deref(),
            Self::AuthenticatedIdentity(identity) => Some(identity.actor()),
        }
    }

    fn authority(&self) -> Result<DeploymentAuthority, Diagnostic> {
        if self.actor().is_some_and(|actor| {
            actor.is_empty()
                || actor.len() > 256
                || actor.trim() != actor
                || actor.chars().any(char::is_control)
        }) {
            return Err(refusal("identity_invalid", "invalid deployment actor"));
        }
        Ok(DeploymentAuthority {
            kind: match self {
                Self::StorageOwner { .. } => AuthorityKind::StorageOwner,
                Self::AuthenticatedIdentity(_) => AuthorityKind::AuthenticatedIdentity,
            },
            actor: self.actor().map(str::to_owned),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorityKind {
    StorageOwner,
    AuthenticatedIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentAuthority {
    pub kind: AuthorityKind,
    pub actor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AchievedDeploymentBase {
    pub result_revision: u64,
    pub resource_digests: BTreeMap<String, String>,
    pub schema_contracts: BTreeMap<String, omnigraph::db::SchemaContractDigest>,
    pub capture_cas: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeploymentAuthorization {
    pub version: u32,
    pub ledger_id: String,
    pub authority: DeploymentAuthority,
    pub base: AchievedDeploymentBase,
    pub input_digest: String,
    pub policy_digests: BTreeMap<String, String>,
    pub effects: Vec<AuthorizedEffect>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeploymentBundle {
    pub version: u32,
    pub canonical_root: String,
    pub config_digest: String,
    pub config_semantics: String,
    pub resources: BTreeMap<String, StateResource>,
    pub sources: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OutstandingDeployment {
    pub id: String,
    pub input_digest: String,
    pub authorization: DeploymentAuthorization,
    pub graphs: BTreeMap<String, GraphDeployment>,
    pub reserved_ledger_bytes: usize,
    pub reserved_result_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GraphDeployment {
    pub intent: Option<PreparedSchemaApply>,
    pub observed_manifest_version: u64,
    pub state: GraphDeploymentState,
    pub settlement: Option<PreparedSchemaSettlement>,
    pub recovery_executor: Option<DeploymentAuthority>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum GraphDeploymentState {
    NotStarted,
    Started,
    Settled { result: Box<GraphDeploymentResult> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum GraphDeploymentResult {
    Schema {
        result: SchemaApplySettlement,
    },
    QueryOnly {
        graph_manifest_version: u64,
        schema_digest: String,
    },
    Refused {
        code: String,
    },
    NotAttempted,
}

impl GraphDeploymentResult {
    fn accepted(&self) -> bool {
        matches!(
            self,
            Self::Schema {
                result: SchemaApplySettlement::Committed { .. }
                    | SchemaApplySettlement::NoOp { .. }
            } | Self::QueryOnly { .. }
        )
    }

    fn terminal(&self) -> bool {
        !matches!(
            self,
            Self::Schema {
                result: SchemaApplySettlement::Unknown
            }
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentResult {
    pub id: String,
    pub input_digest: String,
    pub authority: DeploymentAuthority,
    pub base: AchievedDeploymentBase,
    pub result_revision: u64,
    pub config_digest: Option<String>,
    pub graphs: BTreeMap<String, GraphDeploymentResult>,
    pub recovery_executors: BTreeMap<String, DeploymentAuthority>,
    pub converged: bool,
    pub restart_required: bool,
}

/// A bounded snapshot. Lookup never opens a graph or resumes execution.
#[derive(Debug, Clone, Serialize)]
pub struct DeploymentStatus {
    pub canonical_root: String,
    pub ledger_id: String,
    pub state_revision: u64,
    pub result_revision: u64,
    pub next_sequence: u64,
    pub lock_id: Option<String>,
    pub outstanding_id: Option<String>,
    pub lookup: Option<DeploymentLookup>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum DeploymentLookup {
    Outstanding {
        id: String,
        graphs: BTreeMap<String, String>,
    },
    Complete {
        result: DeploymentResult,
    },
    IdentityMismatch,
    /// The sequence was consumed, but the requested nonce's acceptance and
    /// outcome can no longer be established. This is never retry permission.
    ResultExpired {
        acceptance: &'static str,
        outcome: &'static str,
    },
    NotRecorded,
    DifferentLedger,
}

#[derive(Debug)]
struct DeploymentId {
    ledger: String,
    sequence: u64,
}

impl DeploymentId {
    fn parse(value: &str) -> Result<Self, Diagnostic> {
        if value.len() > 75 {
            return Err(refusal(
                "deployment_id_invalid",
                "invalid deployment identity",
            ));
        }
        let mut parts = value.split(':');
        let (Some(ledger), Some(sequence), Some(nonce), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(refusal(
                "deployment_id_invalid",
                "invalid deployment identity",
            ));
        };
        let canonical_ulid =
            |part: &str| part.parse::<Ulid>().is_ok_and(|id| id.to_string() == part);
        let parsed_sequence = sequence
            .parse::<u64>()
            .ok()
            .filter(|n| *n > 0 && n.to_string() == sequence);
        if !canonical_ulid(ledger) || !canonical_ulid(nonce) || parsed_sequence.is_none() {
            return Err(refusal(
                "deployment_id_invalid",
                "invalid deployment identity",
            ));
        }
        Ok(Self {
            ledger: ledger.to_owned(),
            sequence: parsed_sequence.unwrap(),
        })
    }
}

fn refusal(code: &str, message: impl Into<String>) -> Diagnostic {
    let mut message = message.into();
    const SUFFIX: &str = " [truncated]";
    if message.len() > MAX_DIAGNOSTIC_BYTES {
        let mut end = MAX_DIAGNOSTIC_BYTES - SUFFIX.len();
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message.truncate(end);
        message.push_str(SUFFIX);
    }
    Diagnostic::error(code, CLUSTER_STATE_FILE, message)
}

fn encoded_size(value: &impl Serialize) -> Result<usize, Diagnostic> {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len())
        .map_err(|error| refusal("deployment_encode", error.to_string()))
}

fn valid_actor(actor: Option<&str>) -> bool {
    actor.is_none_or(|actor| {
        !actor.is_empty()
            && actor.len() <= 256
            && actor.trim() == actor
            && !actor.chars().any(char::is_control)
    })
}

fn valid_authority(authority: &DeploymentAuthority) -> bool {
    valid_actor(authority.actor.as_deref())
        && (authority.kind != AuthorityKind::AuthenticatedIdentity || authority.actor.is_some())
}

fn valid_resource_name(value: &str) -> bool {
    let mut diagnostics = Vec::new();
    config::validate_id("resource", "resource", value, &mut diagnostics);
    diagnostics.is_empty()
}

fn valid_address(address: &str) -> bool {
    address.len() <= 512
        && match resource_kind(address) {
            ResourceKind::Graph(id)
            | ResourceKind::Schema(id)
            | ResourceKind::Policy(id)
            | ResourceKind::EmbeddingProvider(id) => valid_resource_name(&id),
            ResourceKind::Query { graph, name } => {
                valid_resource_name(&graph) && valid_resource_name(&name)
            }
            ResourceKind::Unknown => false,
        }
}

fn valid_digests(digests: &BTreeMap<String, String>) -> bool {
    digests.len() <= MAX_RESOURCES
        && digests
            .iter()
            .all(|(address, digest)| valid_address(address) && authorization::valid_digest(digest))
}

fn valid_base(base: &AchievedDeploymentBase) -> bool {
    valid_digests(&base.resource_digests)
        && valid_contracts(&base.schema_contracts, &base.resource_digests)
        && base
            .capture_cas
            .strip_prefix("sha256:")
            .is_some_and(authorization::valid_digest)
}

fn valid_contracts(
    contracts: &BTreeMap<String, omnigraph::db::SchemaContractDigest>,
    resources: &BTreeMap<String, String>,
) -> bool {
    let graphs: BTreeSet<_> = resources
        .keys()
        .filter_map(|address| address.strip_prefix("graph."))
        .collect();
    graphs.len() == contracts.len()
        && contracts.iter().all(|(graph, contract)| {
            graphs.contains(graph.as_str())
                && resources.get(&schema_address(graph)) == Some(&contract.source_hash)
                && authorization::valid_digest(&contract.source_hash)
                && contract
                    .schema_ir_hash
                    .strip_prefix("sha256:")
                    .is_some_and(authorization::valid_digest)
                && contract
                    .schema_identity_domain
                    .parse::<Ulid>()
                    .is_ok_and(|id| id.to_string() == contract.schema_identity_domain)
                && contract.schema_identity_version == 2
        })
}

fn validate_projection(state: &ClusterState) -> Result<(), Diagnostic> {
    let resources = &state.applied_revision.resources;
    for (address, resource) in resources {
        if !valid_address(address) {
            return Err(refusal("invalid_state", "invalid applied resource address"));
        }
        match resource_kind(address) {
            ResourceKind::Graph(graph) => {
                if !resources.contains_key(&schema_address(&graph))
                    || resource
                        .embedding_provider
                        .as_ref()
                        .is_some_and(|provider| {
                            !matches!(resource_kind(provider), ResourceKind::EmbeddingProvider(_))
                                || !resources.contains_key(provider)
                        })
                    || resource.applies_to.is_some()
                    || resource.embedding_profile.is_some()
                {
                    return Err(refusal(
                        "invalid_state",
                        "incomplete applied graph projection",
                    ));
                }
            }
            ResourceKind::Schema(graph) | ResourceKind::Query { graph, .. } => {
                if !resources.contains_key(&graph_address(&graph))
                    || resource.applies_to.is_some()
                    || resource.embedding_provider.is_some()
                    || resource.embedding_profile.is_some()
                    || resource.external_blob_policy.is_some()
                {
                    return Err(refusal(
                        "invalid_state",
                        "invalid applied schema/query projection",
                    ));
                }
            }
            ResourceKind::Policy(_) => {
                if resource.applies_to.as_ref().is_none_or(|bindings| {
                    bindings.is_empty()
                        || bindings.len() > MAX_RESOURCES
                        || bindings.iter().any(|binding| {
                            binding != "cluster"
                                && (!matches!(resource_kind(binding), ResourceKind::Graph(_))
                                    || !resources.contains_key(binding))
                        })
                }) || resource.embedding_provider.is_some()
                    || resource.embedding_profile.is_some()
                    || resource.external_blob_policy.is_some()
                {
                    return Err(refusal("invalid_state", "invalid applied policy bindings"));
                }
            }
            ResourceKind::EmbeddingProvider(_) => {
                if resource
                    .embedding_profile
                    .as_ref()
                    .is_none_or(|profile| embedding_provider_digest(profile) != resource.digest)
                    || resource.applies_to.is_some()
                    || resource.embedding_provider.is_some()
                    || resource.external_blob_policy.is_some()
                {
                    return Err(refusal("invalid_state", "invalid applied provider binding"));
                }
            }
            ResourceKind::Unknown => unreachable!("address was validated"),
        }
    }
    let mut diagnostics = Vec::new();
    if !validate_state_graph_resource_digests(state, &mut diagnostics) {
        return Err(diagnostics.remove(0));
    }
    Ok(())
}

pub(crate) fn validate_state(state: &ClusterState) -> Result<(), Diagnostic> {
    if state.version == 1 {
        if state.mode.is_some()
            || state.ledger_id.is_some()
            || state.next_sequence.is_some()
            || state.outstanding.is_some()
            || state.deployment_results.is_some()
            || state.applied_revision.result_revision.is_some()
            || state.applied_revision.schema_contracts.is_some()
        {
            return Err(refusal(
                "invalid_state_version",
                "v1 ledger carries v2 authority",
            ));
        }
        return Ok(());
    }
    if state.version != 2 || state.mode != Some(DeploymentMode::Offline) {
        return Err(refusal(
            "unsupported_state_version",
            "only ledger v1 and offline v2 are supported",
        ));
    }
    let ledger_id = state
        .ledger_id
        .as_deref()
        .ok_or_else(|| refusal("invalid_state", "missing ledger identity"))?;
    if !ledger_id
        .parse::<Ulid>()
        .is_ok_and(|id| id.to_string() == ledger_id)
        || state.next_sequence.is_none_or(|sequence| sequence == 0)
        || state.applied_revision.result_revision.is_none()
        || state.applied_revision.schema_contracts.is_none()
        || state.applied_revision.resources.len() > MAX_RESOURCES
        || state
            .applied_revision
            .resources
            .iter()
            .any(|(address, resource)| {
                address.len() > 512 || !authorization::valid_digest(&resource.digest)
            })
    {
        return Err(refusal(
            "invalid_state",
            "invalid v2 ledger identity or bounds",
        ));
    }
    let next = state.next_sequence.unwrap();
    validate_projection(state)?;
    if !valid_contracts(
        state.applied_revision.schema_contracts.as_ref().unwrap(),
        &state_resource_digests(state),
    ) {
        return Err(refusal(
            "invalid_state",
            "applied schema contracts do not match the exact graph inventory",
        ));
    }
    if state
        .applied_revision
        .config_digest
        .as_deref()
        .is_some_and(|digest| !authorization::valid_digest(digest))
    {
        return Err(refusal(
            "invalid_state",
            "invalid applied configuration digest",
        ));
    }
    let results = state
        .deployment_results
        .as_ref()
        .ok_or_else(|| refusal("invalid_state", "missing bounded results"))?;
    if results.len() > MAX_RESULTS || encoded_size(results)? > MAX_RESULTS_BYTES {
        return Err(refusal(
            "deployment_bounds",
            "retained results exceed their bound",
        ));
    }
    let mut seen = BTreeSet::new();
    for result in results {
        let id = DeploymentId::parse(&result.id)?;
        if id.ledger != ledger_id
            || id.sequence >= next
            || !seen.insert(id.sequence)
            || encoded_size(result)? > MAX_RESULT_BYTES
            || result.graphs.len() > MAX_RESOURCES
            || result.graphs.values().any(|outcome| !outcome.terminal())
            || result
                .graphs
                .values()
                .filter_map(|outcome| match outcome {
                    GraphDeploymentResult::Refused { code } => Some(code.len()),
                    _ => None,
                })
                .sum::<usize>()
                > MAX_DIAGNOSTIC_BYTES
            || result.graphs.values().any(|outcome| match outcome {
                GraphDeploymentResult::Refused { code } => {
                    code.len() > 128
                        || !code
                            .bytes()
                            .all(|byte| byte.is_ascii_lowercase() || byte == b'_')
                }
                GraphDeploymentResult::QueryOnly {
                    graph_manifest_version,
                    schema_digest,
                } => *graph_manifest_version == 0 || !authorization::valid_digest(schema_digest),
                _ => false,
            })
            || !valid_authority(&result.authority)
            || !valid_base(&result.base)
            || !authorization::valid_digest(&result.input_digest)
            || result
                .config_digest
                .as_deref()
                .is_some_and(|digest| !authorization::valid_digest(digest))
            || result.result_revision > state.applied_revision.result_revision.unwrap()
            || result
                .graphs
                .keys()
                .any(|graph| !valid_resource_name(graph))
            || result.recovery_executors.iter().any(|(graph, authority)| {
                !result.graphs.contains_key(graph) || !valid_authority(authority)
            })
        {
            return Err(refusal(
                "invalid_state",
                "invalid terminal deployment result",
            ));
        }
    }
    if let Some(pending) = &state.outstanding {
        let id = DeploymentId::parse(&pending.id)?;
        if id.ledger != ledger_id
            || id.sequence.checked_add(1) != Some(next)
            || seen.contains(&id.sequence)
            || pending.authorization.ledger_id != ledger_id
            || pending.authorization.version != 2
            || pending.authorization.input_digest != pending.input_digest
            || !authorization::valid_digest(&pending.input_digest)
            || pending.graphs.len() > MAX_RESOURCES
            || pending.reserved_ledger_bytes > MAX_LEDGER_BYTES
            || pending.reserved_result_bytes > MAX_RESULT_BYTES
            || !valid_authority(&pending.authorization.authority)
            || !valid_base(&pending.authorization.base)
            || pending.authorization.base.result_revision
                != state.applied_revision.result_revision.unwrap()
            || pending.authorization.base.resource_digests != state_resource_digests(state)
            || Some(&pending.authorization.base.schema_contracts)
                != state.applied_revision.schema_contracts.as_ref()
            || !valid_digests(&pending.authorization.policy_digests)
            || pending.authorization.effects.len() > MAX_RESOURCES
            || pending.authorization.effects.iter().any(|effect| {
                !valid_address(&effect.resource)
                    || !matches!(effect.operation.as_str(), "create" | "update" | "delete")
                    || effect
                        .before_digest
                        .as_deref()
                        .is_some_and(|digest| !authorization::valid_digest(digest))
                    || effect
                        .after_digest
                        .as_deref()
                        .is_some_and(|digest| !authorization::valid_digest(digest))
                    || effect.binding_change
                    || effect.metadata_change.is_some()
            })
            || pending.graphs.iter().any(|(graph, entry)| {
                !valid_resource_name(graph)
                    || !state
                        .applied_revision
                        .resources
                        .contains_key(&graph_address(graph))
                    || entry.observed_manifest_version == 0
                    || entry.intent.as_ref().is_some_and(|intent| {
                        intent.actor() != pending.authorization.authority.actor.as_deref()
                            || intent.base_manifest_version() != entry.observed_manifest_version
                    })
                    || (entry.intent.is_none()
                        && (entry.settlement.is_some()
                            || matches!(entry.state, GraphDeploymentState::Started)))
                    || entry
                        .recovery_executor
                        .as_ref()
                        .is_some_and(|authority| !valid_authority(authority))
            })
            || encoded_size(state)? > pending.reserved_ledger_bytes
            || pending.reserved_result_bytes == 0
        {
            return Err(refusal(
                "invalid_state",
                "invalid outstanding deployment authority",
            ));
        }
    }
    Ok(())
}
