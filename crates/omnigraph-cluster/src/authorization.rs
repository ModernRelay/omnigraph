//! Configuration-owned authorization for callers that already authenticated
//! an identity. The applied ledger owns policy; candidate files never do.

use std::io::Read;
use std::sync::Arc;

use omnigraph_policy::{PolicyAction, PolicyEngine, PolicyRequest};

use super::*;

const MAX_AUTHORIZATION_RESOURCES: usize = 4096;
const MAX_POLICY_BYTES: usize = 1_048_576;
const MAX_POLICY_TOTAL_BYTES: usize = 8_388_608;

/// Identity supplied by a trusted authentication boundary, not request data.
/// Storage-holding embedders remain responsible for establishing that boundary.
#[derive(Debug, Clone)]
pub struct IdentityAuthorization {
    actor: String,
    bootstrap: Option<BootstrapAuthorization>,
}

impl IdentityAuthorization {
    pub fn authenticated(actor: impl Into<String>) -> Result<Self, Diagnostic> {
        let actor = actor.into();
        if actor.is_empty()
            || actor.len() > 256
            || actor.trim() != actor
            || actor.chars().any(char::is_control)
        {
            return Err(refusal(
                "identity_invalid",
                "actor",
                "authenticated actor is invalid",
            ));
        }
        Ok(Self {
            actor,
            bootstrap: None,
        })
    }

    /// Explicit first-initialization authority, already verified by the caller.
    /// Never derive this capability from an absent policy or an empty graph list.
    /// The caller must bind it to its exact initialization request and must not
    /// issue it for normal apply, restore, or recovery.
    pub fn bootstrap(
        actor: impl Into<String>,
        initial_config_digest: String,
        initial_resource_digests: BTreeMap<String, String>,
    ) -> Result<Self, Diagnostic> {
        let mut identity = Self::authenticated(actor)?;
        if !valid_digest(&initial_config_digest)
            || initial_resource_digests.len() > MAX_AUTHORIZATION_RESOURCES
            || initial_resource_digests
                .iter()
                .any(|(address, digest)| address.len() > 512 || !valid_digest(digest))
        {
            return Err(refusal(
                "bootstrap_authority_invalid",
                "bootstrap",
                "initial configuration authority is invalid",
            ));
        }
        identity.bootstrap = Some(BootstrapAuthorization {
            initial_config_digest,
            initial_resource_digests,
        });
        Ok(identity)
    }

    /// Derive an explicitly authorized initialization from local source files.
    /// Like [`Self::bootstrap`], the caller must already hold exact trusted
    /// bootstrap authority. This constructor performs no remote state or graph
    /// reads; the authorized operation checks the pristine base before preview.
    pub fn bootstrap_config_dir(
        actor: impl Into<String>,
        config_dir: impl AsRef<Path>,
    ) -> Result<Self, Diagnostic> {
        let outcome = load_desired(config_dir.as_ref());
        if let Some(diagnostic) = outcome
            .diagnostics
            .into_iter()
            .find(|diagnostic| diagnostic.severity == DiagnosticSeverity::Error)
        {
            return Err(diagnostic);
        }
        let desired = outcome.desired.ok_or_else(|| {
            refusal(
                "configuration_invalid",
                CLUSTER_CONFIG_FILE,
                "initial configuration is unavailable",
            )
        })?;
        Self::bootstrap(actor, desired.config_digest, desired.resource_digests)
    }

    pub fn actor(&self) -> &str {
        &self.actor
    }
}

#[derive(Debug, Clone)]
struct BootstrapAuthorization {
    initial_config_digest: String,
    initial_resource_digests: BTreeMap<String, String>,
}

/// Exact candidate effect identity. Execution dispositions and migration
/// previews are derived; this binds every resource operation and both digests.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AuthorizedEffect {
    pub resource: String,
    pub operation: String,
    pub before_digest: Option<String>,
    pub after_digest: Option<String>,
    pub binding_change: bool,
    pub metadata_change: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PolicyAuthorizationCheck {
    pub resource: String,
    pub action: String,
    pub branch: Option<String>,
    pub target_branch: Option<String>,
}

/// Evidence of policy evaluation, not a bearer capability. Apply rechecks the
/// actor and current applied policy under its lock before any effect.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PlanAuthorization {
    pub version: u8,
    pub actor: String,
    pub canonical_root: String,
    pub state_revision: u64,
    pub state_cas: Option<String>,
    pub applied_config_digest: Option<String>,
    pub desired_config_digest: String,
    pub policy_digests: BTreeMap<String, String>,
    pub effects: Vec<AuthorizedEffect>,
    pub checks: Vec<PolicyAuthorizationCheck>,
    pub bootstrap: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct AuthorizedPlanOutput {
    pub plan: PlanOutput,
    pub authorization: Option<PlanAuthorization>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AuthorizedApplyOutput {
    pub apply: ApplyOutput,
    /// `None` guarantees this call stopped before recovery, graph or catalog
    /// effects (it may have acquired/released the cluster lock). `Some` records
    /// completed preflight; later failure may have effects and needs the normal
    /// recovery analysis. This says nothing about effects from earlier calls.
    pub authorization: Option<PlanAuthorization>,
}

/// Current applied-policy evidence for a protected result projection.
#[derive(Debug, Clone, Serialize)]
pub struct PlanReadAuthorization {
    pub actor: String,
    pub canonical_root: String,
    pub state_revision: u64,
    pub state_cas: String,
    pub applied_config_digest: Option<String>,
    pub policy_digests: BTreeMap<String, String>,
    pub graphs: Vec<String>,
}

pub(crate) struct AppliedPolicies {
    cluster: PolicyEngine,
    graphs: BTreeMap<String, Arc<PolicyEngine>>,
    digests: BTreeMap<String, String>,
}

impl AppliedPolicies {
    pub(crate) async fn load(
        backend: &ClusterStore,
        state: &ClusterState,
    ) -> Result<Self, Diagnostic> {
        if state.applied_revision.resources.len() > MAX_AUTHORIZATION_RESOURCES {
            return Err(refusal(
                "policy_bounds_exceeded",
                CLUSTER_STATE_FILE,
                "applied resource count exceeds the authorization bound",
            ));
        }
        let mut cluster = None;
        let mut graphs = BTreeMap::new();
        let mut digests = BTreeMap::new();
        let mut total_bytes = 0usize;
        for (address, entry) in &state.applied_revision.resources {
            let kind = resource_kind(address);
            if !matches!(kind, ResourceKind::Policy(_)) {
                continue;
            }
            let bindings = entry.applies_to.as_ref().ok_or_else(|| {
                refusal(
                    "applied_policy_invalid",
                    address,
                    "applied policy has no scope bindings",
                )
            })?;
            let source = backend
                .read_verified_payload_bounded(
                    &kind,
                    &entry.digest,
                    address,
                    MAX_POLICY_BYTES.min(MAX_POLICY_TOTAL_BYTES.saturating_sub(total_bytes)),
                )
                .await?;
            total_bytes = total_bytes.saturating_add(source.len());
            if source.len() > MAX_POLICY_BYTES || total_bytes > MAX_POLICY_TOTAL_BYTES {
                return Err(refusal(
                    "policy_bounds_exceeded",
                    address,
                    "applied policy bytes exceed the authorization bound",
                ));
            }
            for binding in bindings {
                if binding == "cluster" {
                    let engine =
                        PolicyEngine::load_cluster_from_source(&source).map_err(|err| {
                            refusal("applied_policy_invalid", address, err.to_string())
                        })?;
                    if cluster.replace(engine).is_some() {
                        return Err(refusal(
                            "applied_policy_invalid",
                            address,
                            "more than one policy binds cluster configuration",
                        ));
                    }
                } else if let Some(graph) = binding.strip_prefix("graph.") {
                    let engine =
                        PolicyEngine::load_graph_from_source(&source, graph).map_err(|err| {
                            refusal("applied_policy_invalid", address, err.to_string())
                        })?;
                    if graphs.insert(graph.to_string(), Arc::new(engine)).is_some() {
                        return Err(refusal(
                            "applied_policy_invalid",
                            address,
                            "more than one policy binds the graph",
                        ));
                    }
                } else {
                    return Err(refusal(
                        "applied_policy_invalid",
                        address,
                        "unrecognized applied policy binding",
                    ));
                }
            }
            digests.insert(address.clone(), entry.digest.clone());
        }
        Ok(Self {
            cluster: cluster.ok_or_else(|| refusal("cluster_policy_required", "cluster", "identity-authorized operations require an applied cluster management policy; migrate existing clusters explicitly"))?,
            graphs,
            digests,
        })
    }

    fn check_cluster(&self, actor: &str) -> Result<(), Diagnostic> {
        check(
            &self.cluster,
            actor,
            PolicyAction::ConfigManage,
            "cluster",
            None,
            None,
        )
    }

    fn check_graph(
        &self,
        actor: &str,
        graph: &str,
        action: PolicyAction,
    ) -> Result<(), Diagnostic> {
        let policy = self.graphs.get(graph).ok_or_else(|| {
            refusal(
                "graph_policy_required",
                graph,
                "protected graph operation requires an applied graph policy",
            )
        })?;
        check(
            policy,
            actor,
            action,
            graph,
            (action == PolicyAction::Read).then_some("main"),
            (action == PolicyAction::SchemaApply).then_some("main"),
        )
    }

    pub(crate) fn graph(&self, graph: &str) -> Option<Arc<PolicyEngine>> {
        self.graphs.get(graph).cloned()
    }
}

fn check(
    policy: &PolicyEngine,
    actor: &str,
    action: PolicyAction,
    resource: &str,
    branch: Option<&str>,
    target: Option<&str>,
) -> Result<(), Diagnostic> {
    let decision = policy
        .authorize(
            actor,
            &PolicyRequest {
                action,
                branch: branch.map(str::to_string),
                target_branch: target.map(str::to_string),
            },
        )
        .map_err(|err| refusal("policy_evaluation_failed", resource, err.to_string()))?;
    if !decision.allowed {
        return Err(refusal("policy_denied", resource, decision.message));
    }
    Ok(())
}

pub(crate) async fn authorize_candidate(
    backend: &ClusterStore,
    desired: &DesiredCluster,
    state: Option<&ClusterState>,
    observations: &StateObservations,
    changes: &[PlanChange],
    identity: &IdentityAuthorization,
    applying: bool,
) -> Result<(PlanAuthorization, Option<AppliedPolicies>), Diagnostic> {
    if !desired.state_lock {
        return Err(refusal(
            "authorization_requires_lock",
            "state.lock",
            "identity-authorized configuration requires the cluster state lock",
        ));
    }
    if changes.len() > MAX_AUTHORIZATION_RESOURCES {
        return Err(refusal(
            "policy_bounds_exceeded",
            "changes",
            "plan effect count exceeds the authorization bound",
        ));
    }
    refuse_pending_recovery(backend).await?;
    let bootstrap = identity.bootstrap.as_ref();
    let mut checks = Vec::new();
    let policies = if let Some(bootstrap) = bootstrap {
        if bootstrap.initial_config_digest != desired.config_digest
            || bootstrap.initial_resource_digests != desired.resource_digests
        {
            return Err(refusal(
                "bootstrap_authority_mismatch",
                "bootstrap",
                "initial configuration differs from the explicitly authorized initialization",
            ));
        }
        if state.is_some_and(|state| {
            state.state_revision > 1
                || !state.applied_revision.resources.is_empty()
                || !state.approval_records.is_empty()
                || !state.recovery_records.is_empty()
                || state.applied_revision.config_digest.as_deref() != Some(&desired.config_digest)
        }) {
            return Err(refusal(
                "bootstrap_already_initialized",
                "bootstrap",
                "bootstrap authority cannot operate on an initialized cluster",
            ));
        }
        // The explicit capability authorizes installation, not candidate-policy
        // self-authorization. Validate that it actually installs an initial
        // management policy for the authenticated creator.
        validate_initial_policy(desired, &identity.actor)?;
        None
    } else {
        let state = state.ok_or_else(|| {
            refusal(
                "cluster_policy_required",
                CLUSTER_STATE_FILE,
                "no applied policy exists; initialization requires explicit bootstrap authority",
            )
        })?;
        let policies = AppliedPolicies::load(backend, state).await?;
        let mut graph_checks = BTreeSet::new();
        // Resource effects omit source bindings and top-level configuration
        // metadata. Prove those parsed semantics stayed unchanged by replaying
        // the canonical digest with the accepted resource digests. A schema
        // content edit alone can then retain its narrower graph permission.
        let original_resources = state_resource_digests(state);
        let original_config =
            desired_config_digest_from_semantics(&desired.config_semantics, &original_resources);
        let mut needs_config =
            state.applied_revision.config_digest.as_deref() != Some(&original_config);
        for change in changes {
            match resource_kind(&change.resource) {
                ResourceKind::Schema(graph)
                    if state
                        .applied_revision
                        .resources
                        .contains_key(&graph_address(&graph)) =>
                {
                    graph_checks.insert(graph);
                }
                ResourceKind::Graph(graph)
                    if change.operation == PlanOperation::Update
                        && change.metadata_change.is_none() =>
                {
                    // Composite digest follows its independently checked schema
                    // and query effects; it is not separate authority.
                    let metadata_unchanged = state
                        .applied_revision
                        .resources
                        .get(&change.resource)
                        .zip(
                            desired
                                .graphs
                                .iter()
                                .find(|candidate| candidate.id == graph),
                        )
                        .is_some_and(|(applied, candidate)| {
                            applied.embedding_provider == candidate.embedding_provider
                                && applied.external_blob_policy.clone().unwrap_or_default()
                                    == candidate.external_blob_policy
                        });
                    let has_resource_effect = changes.iter().any(|other| {
                        other.resource == schema_address(&graph)
                            || matches!(
                                resource_kind(&other.resource),
                                ResourceKind::Query { graph: ref query_graph, .. }
                                    if query_graph == &graph
                            )
                    });
                    if !metadata_unchanged || !has_resource_effect {
                        needs_config = true;
                    }
                }
                _ => needs_config = true,
            }
        }
        if needs_config {
            policies.check_cluster(&identity.actor)?;
            checks.push(PolicyAuthorizationCheck {
                resource: "cluster".to_string(),
                action: "config_manage".to_string(),
                branch: None,
                target_branch: None,
            });
        }
        for graph in graph_checks {
            policies.check_graph(&identity.actor, &graph, PolicyAction::Read)?;
            checks.push(PolicyAuthorizationCheck {
                resource: graph_address(&graph),
                action: "read".to_string(),
                branch: Some("main".to_string()),
                target_branch: None,
            });
            if applying {
                policies.check_graph(&identity.actor, &graph, PolicyAction::SchemaApply)?;
                checks.push(PolicyAuthorizationCheck {
                    resource: graph_address(&graph),
                    action: "schema_apply".to_string(),
                    branch: None,
                    target_branch: Some("main".to_string()),
                });
            }
        }
        // A read-write graph open also owns its recovery sweep. This caller
        // was authorized for the candidate effects, not an interrupted
        // writer's older data/schema/branch effects. Inspect every affected
        // existing graph before any member of the candidate can begin. The
        // apply invocation repeats this under its cluster lock; callers still
        // retain the existing external graph-writer exclusion contract.
        let affected_graphs = changes
            .iter()
            .filter_map(|change| match resource_kind(&change.resource) {
                ResourceKind::Graph(graph) | ResourceKind::Schema(graph) => Some(graph),
                ResourceKind::Query { graph, .. } => Some(graph),
                _ => None,
            })
            .filter(|graph| {
                state
                    .applied_revision
                    .resources
                    .contains_key(&graph_address(graph))
            })
            .collect::<BTreeSet<_>>();
        for graph in affected_graphs {
            Omnigraph::ensure_no_pending_recovery(&backend.graph_root(&graph))
                .await
                .map_err(|error| {
                    refusal(
                        "policy_recovery_required",
                        graph_address(&graph),
                        error.to_string(),
                    )
                })?;
        }
        Some(policies)
    };
    let evidence = PlanAuthorization {
        version: 1,
        actor: identity.actor.clone(),
        canonical_root: backend.canonical_root()?,
        state_revision: observations.state_revision,
        state_cas: observations.state_cas.clone(),
        applied_config_digest: observations.applied_config_digest.clone(),
        desired_config_digest: desired.config_digest.clone(),
        policy_digests: policies
            .as_ref()
            .map(|p| p.digests.clone())
            .unwrap_or_default(),
        effects: changes.iter().map(effect).collect(),
        checks,
        bootstrap: bootstrap.is_some(),
    };
    Ok((evidence, policies))
}

fn validate_initial_policy(desired: &DesiredCluster, actor: &str) -> Result<(), Diagnostic> {
    let (address, _) = desired
        .policy_bindings
        .iter()
        .find(|(_, bindings)| bindings.iter().any(|scope| scope == "cluster"))
        .ok_or_else(|| {
            refusal(
                "bootstrap_policy_required",
                "cluster",
                "initialization must explicitly declare a cluster management policy",
            )
        })?;
    let resource = desired
        .resources
        .iter()
        .find(|resource| &resource.address == address)
        .ok_or_else(|| {
            refusal(
                "bootstrap_policy_required",
                address,
                "initial policy source missing",
            )
        })?;
    let file = fs::File::open(resource.path.as_ref().ok_or_else(|| {
        refusal(
            "bootstrap_policy_required",
            address,
            "initial policy source missing",
        )
    })?)
    .map_err(|err| refusal("bootstrap_policy_required", address, err.to_string()))?;
    let mut source = String::new();
    file.take(MAX_POLICY_BYTES as u64 + 1)
        .read_to_string(&mut source)
        .map_err(|err| refusal("bootstrap_policy_required", address, err.to_string()))?;
    if source.len() > MAX_POLICY_BYTES || sha256_hex(source.as_bytes()) != resource.digest {
        return Err(refusal(
            "resource_content_changed",
            address,
            "initial policy source differs from its authorized digest",
        ));
    }
    let policy = PolicyEngine::load_cluster_from_source(&source)
        .map_err(|err| refusal("bootstrap_policy_required", address, err.to_string()))?;
    check(
        &policy,
        actor,
        PolicyAction::ConfigManage,
        "cluster",
        None,
        None,
    )
}

pub(crate) fn compare_authorization(
    expected: &PlanAuthorization,
    actual: &PlanAuthorization,
) -> Result<(), Diagnostic> {
    let same_base = expected.state_revision == actual.state_revision
        && expected.state_cas == actual.state_cas
        && expected.applied_config_digest == actual.applied_config_digest;
    // The existing explicit import initializes only the empty ledger between
    // an absent-root bootstrap plan and its apply. No normal path gets this.
    let bootstrap_import = expected.bootstrap
        && actual.bootstrap
        && expected.state_revision == 0
        && expected.state_cas.is_none()
        && actual.state_revision == 1
        && actual.applied_config_digest.as_ref() == Some(&actual.desired_config_digest);
    if expected.version != 1
        || expected.effects.len() > MAX_AUTHORIZATION_RESOURCES
        || expected.canonical_root != actual.canonical_root
        || expected.desired_config_digest != actual.desired_config_digest
        || expected.policy_digests != actual.policy_digests
        || expected.effects != actual.effects
        || expected.bootstrap != actual.bootstrap
        || !(same_base || bootstrap_import)
    {
        return Err(refusal(
            "plan_authorization_stale",
            "authorization",
            "exact plan, actor, root or applied policy revision changed; obtain a fresh authorized plan",
        ));
    }
    Ok(())
}

fn effect(change: &PlanChange) -> AuthorizedEffect {
    AuthorizedEffect {
        resource: change.resource.clone(),
        operation: match change.operation {
            PlanOperation::Create => "create",
            PlanOperation::Update => "update",
            PlanOperation::Delete => "delete",
        }
        .to_string(),
        before_digest: change.before_digest.clone(),
        after_digest: change.after_digest.clone(),
        binding_change: change.binding_change,
        metadata_change: change.metadata_change.map(|metadata| {
            match metadata {
                PlanMetadataChange::PolicyBindings => "policy_bindings",
                PlanMetadataChange::EmbeddingProfile => "embedding_profile",
            }
            .to_string()
        }),
    }
}

pub(crate) async fn refuse_pending_recovery(backend: &ClusterStore) -> Result<(), Diagnostic> {
    let mut diagnostics = Vec::new();
    let pending = backend
        .list_recovery_sidecar_locations(&mut diagnostics)
        .await;
    if !pending.is_empty() || !diagnostics.is_empty() {
        return Err(refusal(
            "policy_recovery_required",
            CLUSTER_RECOVERIES_DIR,
            "pending or uncertain recovery must be resolved under its original authority before a new identity-authorized plan",
        ));
    }
    Ok(())
}

/// Reauthorize a protected historical plan/result projection using CURRENT
/// applied policy. `graphs` must enumerate every remote schema represented by
/// that projection; the saved author's identity or receipt is never a read grant.
/// The caller must not release the projection when this function refuses.
pub async fn authorize_plan_read(
    storage_root: &str,
    identity: &IdentityAuthorization,
    graphs: &[String],
) -> Result<PlanReadAuthorization, Diagnostic> {
    if graphs.len() > MAX_AUTHORIZATION_RESOURCES || identity.bootstrap.is_some() {
        return Err(refusal(
            "plan_read_authority_invalid",
            "graphs",
            "historical projection requires ordinary identity and bounded graph scope",
        ));
    }
    let backend = ClusterStore::for_storage_root(storage_root)?;
    let mut observations = backend.observations();
    let snapshot = backend.read_state(&mut observations).await?;
    let state = snapshot.state.ok_or_else(|| {
        refusal(
            "cluster_policy_required",
            CLUSTER_STATE_FILE,
            "applied cluster state is required",
        )
    })?;
    refuse_pending_recovery(&backend).await?;
    let policies = AppliedPolicies::load(&backend, &state).await?;
    policies.check_cluster(&identity.actor)?;
    let graphs: BTreeSet<String> = graphs.iter().cloned().collect();
    for graph in &graphs {
        policies.check_graph(&identity.actor, graph, PolicyAction::Read)?;
    }
    Ok(PlanReadAuthorization {
        actor: identity.actor.clone(),
        canonical_root: backend.canonical_root()?,
        state_revision: observations.state_revision,
        state_cas: observations.state_cas.ok_or_else(|| {
            refusal(
                "policy_revision_missing",
                CLUSTER_STATE_FILE,
                "applied state CAS is required",
            )
        })?,
        applied_config_digest: observations.applied_config_digest,
        policy_digests: policies.digests,
        graphs: graphs.into_iter().collect(),
    })
}

/// Effect-free execution preflight for a trusted orchestrator. This checks the
/// complete candidate against current applied policy before the caller writes
/// its own execution artifacts. It does not acquire writer authority: the
/// authorized apply entry point repeats the check under the cluster lock.
pub async fn authorize_apply_plan(
    config_dir: impl AsRef<Path>,
    identity: &IdentityAuthorization,
    expected: &PlanAuthorization,
) -> Result<PlanAuthorization, Diagnostic> {
    let outcome = load_desired(config_dir.as_ref());
    if let Some(diagnostic) = outcome
        .diagnostics
        .into_iter()
        .find(|diagnostic| diagnostic.severity == DiagnosticSeverity::Error)
    {
        return Err(diagnostic);
    }
    let desired = outcome.desired.ok_or_else(|| {
        refusal(
            "configuration_invalid",
            CLUSTER_CONFIG_FILE,
            "candidate configuration is unavailable",
        )
    })?;
    let backend = store_for(&desired.config_dir, desired.storage_root.as_deref())?;
    let mut observations = backend.observations();
    let snapshot = backend.read_state(&mut observations).await?;
    if let Some(state) = &snapshot.state {
        let mut diagnostics = Vec::new();
        if !validate_state_graph_resource_digests(state, &mut diagnostics) {
            return Err(diagnostics.remove(0));
        }
    }
    let prior = snapshot
        .state
        .as_ref()
        .map(state_resource_digests)
        .unwrap_or_default();
    let mut changes = diff_resources(&prior, &desired.resource_digests);
    append_policy_binding_changes(&mut changes, snapshot.state.as_ref(), &desired);
    append_embedding_profile_changes(&mut changes, snapshot.state.as_ref(), &desired);
    let (authorization, _) = authorize_candidate(
        &backend,
        &desired,
        snapshot.state.as_ref(),
        &observations,
        &changes,
        identity,
        true,
    )
    .await?;
    compare_authorization(expected, &authorization)?;
    Ok(authorization)
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}
fn refusal(code: &str, path: impl Into<String>, message: impl Into<String>) -> Diagnostic {
    Diagnostic::error(code, path, message)
}
