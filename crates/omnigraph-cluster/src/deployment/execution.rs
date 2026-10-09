use super::*;
use crate::authorization::AppliedPolicies;
use omnigraph_policy::PolicyAction;
use omnigraph_seams::decide_seam;

pub(super) async fn read_existing(
    store: &ClusterStore,
) -> Result<(ClusterState, String), Diagnostic> {
    let snapshot = store.read_state(&mut store.observations()).await?;
    Ok((
        snapshot
            .state
            .ok_or_else(|| refusal("state_missing", "cluster ledger is missing"))?,
        snapshot
            .state_cas
            .ok_or_else(|| refusal("state_missing", "cluster ledger is missing"))?,
    ))
}

fn require_v2(state: &ClusterState) -> Result<(), Diagnostic> {
    if state.version != 2 {
        return Err(refusal(
            "ledger_upgrade_required",
            "explicitly upgrade the stopped cluster ledger before deploying",
        ));
    }
    validate_state(state)
}

async fn policies(
    store: &ClusterStore,
    state: &ClusterState,
    caller: &DeploymentCaller,
) -> Result<AppliedPolicies, Diagnostic> {
    caller.authority()?;
    let policies = match caller {
        DeploymentCaller::StorageOwner { .. } => {
            AppliedPolicies::load_optional(store, state).await?
        }
        DeploymentCaller::AuthenticatedIdentity(identity) => {
            let policies = AppliedPolicies::load(store, state).await?;
            // Configuration management includes ledger/receipt metadata. Graph
            // Read gates graph data, not unrelated cluster inventory; schema
            // effects enforce their graph actions separately during preflight.
            policies.check_cluster(identity.actor())?;
            policies
        }
    };
    Ok(policies)
}

pub(super) fn authorize_bootstrap_bundle(
    bundle: &DeploymentBundle,
    caller: &DeploymentCaller,
) -> Result<(), Diagnostic> {
    if let DeploymentCaller::AuthenticatedIdentity(identity) = caller {
        identity.check_bootstrap_input(
            &bundle.config_digest,
            &bundle
                .resources
                .iter()
                .map(|(address, resource)| (address.clone(), resource.digest.clone()))
                .collect(),
        )?;
        let address = bundle
            .resources
            .iter()
            .find(|(_, resource)| {
                resource
                    .applies_to
                    .as_ref()
                    .is_some_and(|bindings| bindings.iter().any(|binding| binding == "cluster"))
            })
            .map(|(address, _)| address)
            .ok_or_else(|| {
                refusal(
                    "bootstrap_policy_required",
                    "initialization must install a cluster management policy",
                )
            })?;
        let policy =
            omnigraph_policy::PolicyEngine::load_cluster_from_source(source_for(bundle, address)?)
                .map_err(|error| refusal("bootstrap_policy_required", error.to_string()))?;
        let decision = policy
            .authorize(
                identity.actor(),
                &omnigraph_policy::PolicyRequest {
                    action: PolicyAction::ConfigManage,
                    branch: None,
                    target_branch: None,
                },
            )
            .map_err(|error| refusal("policy_evaluation_failed", error.to_string()))?;
        if !decision.allowed {
            return Err(refusal("policy_denied", decision.message));
        }
    }
    Ok(())
}

async fn deployment_policies(
    store: &ClusterStore,
    state: &ClusterState,
    bundle: &DeploymentBundle,
    caller: &DeploymentCaller,
) -> Result<AppliedPolicies, Diagnostic> {
    if let DeploymentCaller::AuthenticatedIdentity(identity) = caller {
        if identity.has_bootstrap_authority() {
            authorize_bootstrap_bundle(bundle, caller)?;
            if !pristine_bootstrap_state(state) {
                return Err(refusal(
                    "bootstrap_already_initialized",
                    "bootstrap authority cannot operate on an initialized cluster",
                ));
            }
            return AppliedPolicies::load_optional(store, state).await;
        }
    }
    if let DeploymentCaller::StorageOwner { .. } = caller {
        return AppliedPolicies::load_optional(store, state).await;
    }
    let applied = AppliedPolicies::load(store, state).await?;
    applied.check_cluster(caller.actor().unwrap())?;
    Ok(applied)
}

// Before first convergence there is no applied management policy. Only the
// exact trusted bootstrap capability may inspect or settle its own accepted
// original invocation; an empty ledger by itself grants no authority.
async fn lookup_policies(
    store: &ClusterStore,
    state: &ClusterState,
    caller: &DeploymentCaller,
    id: Option<&str>,
) -> Result<AppliedPolicies, Diagnostic> {
    if let DeploymentCaller::AuthenticatedIdentity(identity) = caller {
        if identity.has_bootstrap_authority() && state.applied_revision.resources.is_empty() {
            let pending = state
                .outstanding
                .as_ref()
                .filter(|pending| {
                    Some(pending.id.as_str()) == id
                        && pending.authorization.base.resource_digests.is_empty()
                        && pending.authorization.authority.actor.as_deref()
                            == Some(identity.actor())
                })
                .ok_or_else(|| {
                    refusal(
                        "bootstrap_authority_mismatch",
                        "bootstrap capability may observe only its original accepted invocation",
                    )
                })?;
            let bundle = store.read_deployment_bundle(&pending.input_digest).await?;
            validate_bundle(&bundle, &store.canonical_root()?)?;
            authorize_bootstrap_bundle(&bundle, caller)?;
            return AppliedPolicies::load_optional(store, state).await;
        }
    }
    policies(store, state, caller).await
}

fn graph_ids(state: &ClusterState) -> Vec<String> {
    state
        .applied_revision
        .resources
        .keys()
        .filter_map(|address| match resource_kind(address) {
            ResourceKind::Graph(graph) => Some(graph),
            _ => None,
        })
        .collect()
}

/// Ledger-only lookup. Missing/expired evidence never authorizes replay.
pub async fn deployment_status(
    root: &str,
    id: Option<&str>,
    caller: &DeploymentCaller,
) -> Result<DeploymentStatus, Diagnostic> {
    let store = ClusterStore::for_storage_root(root)?;
    let (state, _) = read_existing(&store).await?;
    require_v2(&state)?;
    lookup_policies(&store, &state, caller, id).await?;
    let mut observations = store.observations();
    let mut diagnostics = Vec::new();
    store
        .observe_lock(&mut observations, &mut diagnostics)
        .await;
    if let Some(error) = diagnostics.into_iter().next() {
        return Err(error);
    }
    Ok(DeploymentStatus {
        canonical_root: store.canonical_root()?,
        ledger_id: state.ledger_id.clone().unwrap(),
        state_revision: state.state_revision,
        result_revision: state.applied_revision.result_revision.unwrap(),
        next_sequence: state.next_sequence.unwrap(),
        lock_id: observations.lock_id,
        outstanding_id: state.outstanding_deployment_id().map(str::to_owned),
        lookup: id.map(|id| lookup(&state, id)).transpose()?,
    })
}

/// Read one durable invocation without disclosing the current cluster status.
/// Current management permission or the exact authenticated initiating actor
/// authorizes this receipt only. Missing/expired ownership evidence grants no
/// fallback permission, and storage-owner attribution is never authentication.
pub async fn deployment_receipt(
    root: &str,
    id: &str,
    identity: &IdentityAuthorization,
) -> Result<(DeploymentLookup, u64), Diagnostic> {
    let store = ClusterStore::for_storage_root(root)?;
    let (state, _) = read_existing(&store).await?;
    require_v2(&state)?;
    let authority = state
        .outstanding
        .as_ref()
        .filter(|pending| pending.id == id)
        .map(|pending| &pending.authorization.authority)
        .or_else(|| {
            state
                .deployment_results
                .as_ref()?
                .iter()
                .find(|result| result.id == id)
                .map(|result| &result.authority)
        });
    if !authority.is_some_and(|authority| {
        authority.kind == AuthorityKind::AuthenticatedIdentity
            && authority.actor.as_deref() == Some(identity.actor())
    }) {
        policies(
            &store,
            &state,
            &DeploymentCaller::AuthenticatedIdentity(identity.clone()),
        )
        .await?;
    }
    Ok((
        lookup(&state, id)?,
        state.applied_revision.result_revision.unwrap(),
    ))
}

fn lookup(state: &ClusterState, requested: &str) -> Result<DeploymentLookup, Diagnostic> {
    let id = DeploymentId::parse(requested)?;
    if Some(id.ledger.as_str()) != state.ledger_id.as_deref() {
        return Ok(DeploymentLookup::DifferentLedger);
    }
    if let Some(pending) = &state.outstanding {
        if pending.id == requested {
            return Ok(DeploymentLookup::Outstanding {
                id: requested.to_owned(),
                input_digest: pending.input_digest.clone(),
                graphs: pending
                    .graphs
                    .iter()
                    .map(|(graph, entry)| {
                        (
                            graph.clone(),
                            match entry.state {
                                GraphDeploymentState::NotStarted => "not_started",
                                GraphDeploymentState::Started => "started",
                                GraphDeploymentState::Settled { .. } => "settled",
                            }
                            .to_owned(),
                        )
                    })
                    .collect(),
            });
        }
        if DeploymentId::parse(&pending.id)?.sequence == id.sequence {
            return Ok(DeploymentLookup::IdentityMismatch);
        }
    }
    for result in state.deployment_results.as_ref().unwrap() {
        if result.id == requested {
            return Ok(DeploymentLookup::Complete {
                result: result.clone(),
            });
        }
        if DeploymentId::parse(&result.id)?.sequence == id.sequence {
            return Ok(DeploymentLookup::IdentityMismatch);
        }
    }
    Ok(if id.sequence < state.next_sequence.unwrap() {
        DeploymentLookup::ResultExpired {
            acceptance: "unknown".into(),
            outcome: "unknown".into(),
        }
    } else {
        DeploymentLookup::NotRecorded
    })
}

/// Exact-ID emergency unlock. The operator must exclude new admissions and
/// other releases and establish prior native/control-I/O quiescence first.
pub async fn force_unlock_storage_root(root: &str, lock_id: &str) -> Result<(), Diagnostic> {
    let store = ClusterStore::for_storage_root(root)?;
    store.force_unlock(lock_id, &mut store.observations()).await
}

async fn capture_applied_graph_contracts(
    store: &ClusterStore,
    state: &ClusterState,
) -> Result<BTreeMap<String, omnigraph::db::SchemaContractDigest>, Diagnostic> {
    let mut diagnostics = Vec::new();
    if !validate_state_graph_resource_digests(state, &mut diagnostics) {
        return Err(diagnostics.remove(0));
    }
    let mut contracts = BTreeMap::new();
    for graph in graph_ids(state) {
        let uri = store.graph_root(&graph);
        Omnigraph::ensure_no_pending_recovery(&uri)
            .await
            .map_err(|e| refusal("graph_recovery_required", e.to_string()))?;
        let db = Omnigraph::open_read_only(&uri)
            .await
            .map_err(|e| refusal("graph_unavailable", e.to_string()))?;
        if state
            .applied_revision
            .resources
            .get(&schema_address(&graph))
            .is_none_or(|schema| sha256_hex(db.schema_source().as_bytes()) != schema.digest)
            || state
                .applied_revision
                .schema_contracts
                .as_ref()
                .is_some_and(|contracts| {
                    contracts.get(&graph) != Some(&db.schema_contract_digest())
                })
        {
            return Err(refusal(
                "applied_schema_drift",
                format!("graph {graph} differs from its achieved schema"),
            ));
        }
        contracts.insert(graph, db.schema_contract_digest());
    }
    Ok(contracts)
}

/// Data-preserving ledger conversion, independent of mutable configuration.
/// `writers_stopped` attests external exclusion and settled prior control I/O.
pub async fn upgrade_deployment_ledger(
    root: &str,
    writers_stopped: bool,
    caller: &DeploymentCaller,
) -> Result<DeploymentStatus, Diagnostic> {
    if !writers_stopped {
        return Err(refusal(
            "writers_stopped_required",
            "conversion requires --writers-stopped",
        ));
    }
    let store = ClusterStore::for_storage_root(root)?;
    let (before, conversion) = store.read_state_for_ledger_upgrade().await?;
    let before = before
        .state
        .ok_or_else(|| refusal("state_missing", "cluster ledger is missing"))?;
    if before.outstanding.is_some() {
        return Err(refusal(
            "ledger_upgrade_pending",
            "complete the outstanding deployment with its originating build before ledger conversion",
        ));
    }
    if before.version == 2 && !conversion {
        return deployment_status(root, None, caller).await;
    }
    // Refuse unauthorized conversion before acquiring durable admission; a
    // denied caller must not strand a lock. Recheck current policy under it.
    policies(&store, &before, caller).await?;
    let mut observations = store.observations();
    let mut guard = store
        .acquire_lock("upgrade_ledger", &mut observations)
        .await?;
    // Lost conversion acknowledgement must not silently release v2 admission.
    guard.hold_on_drop();
    // All work in this phase is awaited control reads and read-only graph
    // validation. Only the later replacement may write converted state.
    let preflight = async {
        let (snapshot, conversion) = store.read_state_for_ledger_upgrade().await?;
        let mut state = snapshot
            .state
            .ok_or_else(|| refusal("state_missing", "cluster ledger is missing"))?;
        let cas = snapshot
            .state_cas
            .ok_or_else(|| refusal("state_missing", "cluster ledger is missing"))?;
        if state.version != before.version || (state.version == 2 && !conversion) {
            return Err(refusal(
                "ledger_changed",
                "ledger changed during conversion; inspect original root",
            ));
        }
        policies(&store, &state, caller).await?;
        if state.version == 1 {
            authorization::refuse_pending_recovery(&store).await?;
            serve::read_snapshot_for_ledger_upgrade(&store)
                .await
                .map_err(|mut diagnostics| diagnostics.remove(0))?;
            state.applied_revision.schema_contracts =
                Some(capture_applied_graph_contracts(&store, &state).await?);
            state.version = 2;
            state.ledger_id = Some(Ulid::new().to_string());
            state.next_sequence = Some(1);
            state.applied_revision.result_revision = Some(0);
            state.deployment_results = Some(Vec::new());
        }
        // Validate the exact representation replace() will write, including
        // its incremented revision, while this is still a read-only phase.
        let previous_revision = state.state_revision;
        state.state_revision = previous_revision
            .checked_add(1)
            .ok_or_else(|| refusal("revision_exhausted", "ledger revision exhausted"))?;
        validate_state(&state)?;
        if encoded_size(&state)? > MAX_LEDGER_BYTES {
            return Err(refusal(
                "deployment_bounds",
                "converted ledger exceeds its encoded byte limit",
            ));
        }
        state.state_revision = previous_revision;
        Ok::<_, Diagnostic>((state, cas))
    }
    .await;
    let (mut state, cas) = match preflight {
        Ok(prepared) => prepared,
        Err(error) => {
            return Err(admission::release_refused_preflight(&store, guard.lock_id(), error).await);
        }
    };
    state.state_revision = state
        .state_revision
        .checked_add(1)
        .ok_or_else(|| refusal("revision_exhausted", "ledger revision exhausted"))?;
    store
        .write_state_for_ledger_upgrade(&state, &cas)
        .await
        .map_err(|error| retained_error(error, guard.lock_id()))?;
    store.release_settled(guard.lock_id()).await?;
    deployment_status(root, None, caller).await
}

pub(super) fn empty_ledger() -> ClusterState {
    ClusterState {
        version: 2,
        ledger_id: Some(Ulid::new().to_string()),
        next_sequence: Some(1),
        outstanding: None,
        deployment_results: Some(Vec::new()),
        state_revision: 1,
        applied_revision: AppliedRevisionState {
            schema_contracts: Some(BTreeMap::new()),
            result_revision: Some(0),
            config_digest: None,
            resources: BTreeMap::new(),
        },
        resource_statuses: BTreeMap::new(),
        approval_records: BTreeMap::new(),
        recovery_records: BTreeMap::new(),
        observations: BTreeMap::new(),
    }
}

/// Initialize only an empty root; all graph effects then use the same v2
/// deployment protocol as updates. Existing v1 state is never executed.
async fn bootstrap_ledger(
    store: &ClusterStore,
    bundle: &DeploymentBundle,
    caller: &DeploymentCaller,
) -> Result<(), Diagnostic> {
    caller.authority()?;
    if let Some(state) = store.read_state(&mut store.observations()).await?.state {
        return require_v2(&state);
    }
    authorize_bootstrap_bundle(bundle, caller)?;
    let mut observations = store.observations();
    let mut guard = store.acquire_lock("bootstrap", &mut observations).await?;
    guard.hold_on_drop();
    let checked = async {
        if store
            .read_state(&mut store.observations())
            .await?
            .state
            .is_some()
        {
            return Err(refusal(
                "ledger_changed",
                "cluster initialized while acquiring bootstrap ownership",
            ));
        }
        // Validate the captured input before the first ledger write. Existing
        // unmanaged roots refuse, including after a lost cluster ledger.
        let state = empty_ledger();
        let id = format!("{}:1:{}", state.ledger_id.as_ref().unwrap(), Ulid::new());
        let input_digest = bundle.input_digest()?;
        Box::pin(prepare_deployment(
            store,
            bundle,
            caller,
            (state.clone(), format!("sha256:{}", sha256_hex(b""))),
            &BTreeMap::new(),
            &id,
            &input_digest,
        ))
        .await?;
        Ok::<_, Diagnostic>(state)
    }
    .await;
    let state = match checked {
        Ok(state) => state,
        Err(error) => {
            return Err(admission::release_refused_preflight(store, guard.lock_id(), error).await);
        }
    };
    store
        .write_state(&state, None, &mut observations)
        .await
        .map_err(|error| retained_error(error, guard.lock_id()))?;
    // Only awaited control writes happened. No graph/native operation started.
    store
        .release_settled(guard.lock_id())
        .await
        .map_err(|error| retained_error(error, guard.lock_id()))
}

async fn replace(
    store: &ClusterStore,
    state: &mut ClusterState,
    cas: &str,
) -> Result<String, Diagnostic> {
    state.state_revision = state
        .state_revision
        .checked_add(1)
        .ok_or_else(|| refusal("revision_exhausted", "ledger revision exhausted"))?;
    if let Some(pending) = &state.outstanding {
        if encoded_size(state)? > pending.reserved_ledger_bytes {
            return Err(refusal(
                "deployment_reservation_exceeded",
                "progress exceeds reserved completion capacity",
            ));
        }
    }
    let mut observations = store.observations();
    store
        .write_state(state, Some(cas), &mut observations)
        .await?;
    Ok(observations
        .state_cas
        .expect("confirmed write records its CAS"))
}

fn bundle_from_capture(
    desired: &DesiredCluster,
    sources: BTreeMap<String, std::sync::Arc<str>>,
    root: String,
) -> DeploymentBundle {
    let mut resources = BTreeMap::new();
    for (address, digest) in &desired.resource_digests {
        let mut resource = StateResource {
            digest: digest.clone(),
            applies_to: None,
            embedding_provider: None,
            embedding_profile: None,
            external_blob_policy: None,
        };
        match resource_kind(address) {
            ResourceKind::Policy(_) => {
                resource.applies_to = desired.policy_bindings.get(address).cloned()
            }
            ResourceKind::Graph(graph) => {
                let graph = desired
                    .graphs
                    .iter()
                    .find(|entry| entry.id == graph)
                    .unwrap();
                resource.embedding_provider = graph.embedding_provider.clone();
                resource.external_blob_policy =
                    persisted_external_blob_policy(&graph.external_blob_policy);
            }
            ResourceKind::EmbeddingProvider(_) => {
                resource.embedding_profile = desired.embedding_providers.get(address).cloned()
            }
            _ => {}
        }
        resources.insert(address.clone(), resource);
    }
    DeploymentBundle {
        version: 2,
        canonical_root: root,
        config_digest: desired.config_digest.clone(),
        config_semantics: desired.config_semantics.clone(),
        resources,
        sources: sources
            .into_iter()
            .map(|(digest, source)| (digest, source.to_string()))
            .collect(),
    }
}

pub(super) fn validate_bundle(bundle: &DeploymentBundle, root: &str) -> Result<(), Diagnostic> {
    validate_bundle_with_root(bundle, root, |declared| {
        ClusterStore::for_storage_root(declared)?.canonical_root()
    })
}

fn validate_bundle_with_root(
    bundle: &DeploymentBundle,
    root: &str,
    root_identity: impl Fn(&str) -> Result<String, Diagnostic>,
) -> Result<(), Diagnostic> {
    if bundle.version != 2
        || bundle.canonical_root != root
        || root.len() > 4096
        || bundle.resources.len() > MAX_RESOURCES
        || encoded_size(bundle)? > MAX_BUNDLE_BYTES
        || bundle
            .sources
            .values()
            .any(|source| source.len() > config::MAX_CONFIG_SOURCE_BYTES)
        || bundle.sources.values().map(String::len).sum::<usize>() > config::MAX_CONFIG_TOTAL_BYTES
        || bundle
            .sources
            .iter()
            .any(|(digest, source)| sha256_hex(source.as_bytes()) != *digest)
        || bundle.resources.iter().any(|(address, resource)| {
            address.len() > 512 || !authorization::valid_digest(&resource.digest)
        })
        || desired_config_digest_from_semantics(
            &bundle.config_semantics,
            &bundle
                .resources
                .iter()
                .map(|(address, resource)| (address.clone(), resource.digest.clone()))
                .collect(),
        ) != bundle.config_digest
    {
        return Err(refusal(
            "deployment_input_invalid",
            "immutable deployment input has invalid bounds or identities",
        ));
    }
    // CapturedDeployment is deserializable request data, not trusted output of
    // capture_deployment. Reparse semantic configuration and compare its exact
    // normalized runtime bindings with the submitted resource projection.
    let raw: RawClusterConfig = serde_json::from_str(&bundle.config_semantics)
        .map_err(|error| refusal("deployment_input_invalid", error.to_string()))?;
    if serde_json::to_string(&raw)
        .map_err(|error| refusal("deployment_encode", error.to_string()))?
        != bundle.config_semantics
    {
        return Err(refusal(
            "deployment_input_invalid",
            "configuration semantics must use the canonical captured representation",
        ));
    }
    let mut diagnostics = Vec::new();
    let settings = config::validate_cluster_header(&raw, &mut diagnostics);
    if !settings.state_lock {
        return Err(refusal(
            "deployment_requires_lock",
            "deployment requires cluster admission",
        ));
    }
    if let Some(declared_root) = settings.storage_root {
        if root_identity(&declared_root)? != root {
            return Err(refusal(
                "deployment_input_invalid",
                "declared storage differs from captured root",
            ));
        }
    }
    let graph_ids: BTreeSet<_> = bundle.graph_ids().into_iter().collect();
    if graph_ids != raw.graphs.keys().cloned().collect() {
        return Err(refusal(
            "deployment_input_invalid",
            "graph inventory differs from configuration semantics",
        ));
    }
    let mut bound_scopes = BTreeSet::new();
    for (name, provider) in &raw.providers.embedding {
        let address = config::embedding_provider_address(name);
        provider.validate(address.clone(), &mut diagnostics);
        if bundle
            .resources
            .get(&address)
            .and_then(|resource| resource.embedding_profile.as_ref())
            != Some(provider)
        {
            return Err(refusal(
                "deployment_input_invalid",
                "embedding provider differs from configuration semantics",
            ));
        }
    }
    for (graph, declaration) in &raw.graphs {
        let resource = &bundle.resources[&graph_address(graph)];
        let provider = declaration
            .embedding_provider
            .as_deref()
            .map(config::normalize_embedding_provider_target)
            .map(|target| match target {
                config::EmbeddingProviderTarget::Provider(name) => {
                    Ok(config::embedding_provider_address(&name))
                }
                config::EmbeddingProviderTarget::WrongKind(_) => Err(refusal(
                    "deployment_input_invalid",
                    "invalid provider binding",
                )),
            })
            .transpose()?;
        let policy = config::validate_external_blob_policy(
            graph,
            &declaration.external_blobs,
            Some(root),
            &mut diagnostics,
        );
        if resource.embedding_provider != provider
            || resource.external_blob_policy != persisted_external_blob_policy(&policy)
        {
            return Err(refusal(
                "deployment_input_invalid",
                "graph runtime binding differs from configuration semantics",
            ));
        }
    }
    for (name, declaration) in &raw.policies {
        let mut bindings = declaration
            .applies_to
            .iter()
            .map(|binding| match config::normalize_policy_target(binding) {
                config::PolicyTarget::Cluster => Ok("cluster".to_owned()),
                config::PolicyTarget::Graph(graph) => Ok(graph_address(&graph)),
                config::PolicyTarget::WrongKind(_) => Err(refusal(
                    "deployment_input_invalid",
                    "invalid policy binding",
                )),
            })
            .collect::<Result<Vec<_>, _>>()?;
        bindings.sort();
        bindings.dedup();
        if (bindings.iter().any(|binding| binding == "cluster") && bindings.len() != 1)
            || bindings
                .iter()
                .any(|binding| !bound_scopes.insert(binding.clone()))
            || bundle
                .resources
                .get(&config::policy_address(name))
                .and_then(|resource| resource.applies_to.as_ref())
                != Some(&bindings)
        {
            return Err(refusal(
                "deployment_input_invalid",
                "policy scopes differ from configuration semantics or overlap",
            ));
        }
    }
    if bundle
        .resources
        .keys()
        .any(|address| match resource_kind(address) {
            ResourceKind::Policy(name) => !raw.policies.contains_key(&name),
            ResourceKind::EmbeddingProvider(name) => !raw.providers.embedding.contains_key(&name),
            _ => false,
        })
    {
        return Err(refusal(
            "deployment_input_invalid",
            "undeclared runtime binding resource",
        ));
    }
    if let Some(error) = diagnostics
        .into_iter()
        .find(|error| error.severity == DiagnosticSeverity::Error)
    {
        return Err(error);
    }
    let mut projection = empty_ledger();
    projection.applied_revision.resources = bundle.resources.clone();
    validate_projection(&projection)?;
    for graph in bundle.graph_ids() {
        let schema = parse_schema(source_for(bundle, &schema_address(&graph))?)
            .map_err(|error| refusal("schema_preflight_failed", error.to_string()))?;
        let catalog = build_catalog(&schema)
            .map_err(|error| refusal("schema_preflight_failed", error.to_string()))?;
        for address in bundle.resources.keys() {
            if let ResourceKind::Query { graph: owner, name } = resource_kind(address) {
                if owner == graph {
                    let mut diagnostics = Vec::new();
                    config::validate_query_source(
                        &graph,
                        &name,
                        source_for(bundle, address)?,
                        Some(&catalog),
                        &mut diagnostics,
                    );
                    if let Some(error) = diagnostics
                        .into_iter()
                        .find(|error| error.severity == DiagnosticSeverity::Error)
                    {
                        return Err(error);
                    }
                }
            }
        }
    }
    for (address, resource) in &bundle.resources {
        if matches!(resource_kind(address), ResourceKind::Policy(_)) {
            for binding in resource.applies_to.as_ref().unwrap() {
                let source = source_for(bundle, address)?;
                let checked = if binding == "cluster" {
                    omnigraph_policy::PolicyEngine::load_cluster_from_source(source)
                } else {
                    omnigraph_policy::PolicyEngine::load_graph_from_source(
                        source,
                        binding.strip_prefix("graph.").unwrap(),
                    )
                };
                checked.map_err(|error| refusal("policy_invalid", error.to_string()))?;
            }
        }
    }
    Ok(())
}

pub(crate) fn preview_deployment_scope(
    state: &ClusterState,
    bundle: &DeploymentBundle,
) -> Result<Vec<AuthorizedEffect>, Diagnostic> {
    let before = &state.applied_revision.resources;
    let addresses: BTreeSet<_> = before.keys().chain(bundle.resources.keys()).collect();
    let mut effects = Vec::new();
    for address in addresses {
        let old = before.get(address);
        let new = bundle.resources.get(address);
        let unchanged = serde_json::to_value(old).unwrap() == serde_json::to_value(new).unwrap();
        if unchanged {
            continue;
        }
        if matches!(resource_kind(address), ResourceKind::Unknown) {
            return Err(refusal(
                "deployment_scope",
                format!("unsupported resource {address}"),
            ));
        }
        effects.push(AuthorizedEffect {
            resource: address.clone(),
            operation: if old.is_none() {
                "create"
            } else if new.is_none() {
                "delete"
            } else {
                "update"
            }
            .into(),
            before_digest: old.map(|entry| entry.digest.clone()),
            after_digest: new.map(|entry| entry.digest.clone()),
            binding_change: false,
            metadata_change: None,
        });
    }
    Ok(effects)
}

fn affected_graphs(
    state: &ClusterState,
    bundle: &DeploymentBundle,
    effects: &[AuthorizedEffect],
) -> BTreeSet<String> {
    let mut graphs = BTreeSet::new();
    for effect in effects {
        match resource_kind(&effect.resource) {
            ResourceKind::Graph(graph)
            | ResourceKind::Schema(graph)
            | ResourceKind::Query { graph, .. } => {
                graphs.insert(graph);
            }
            ResourceKind::Policy(_) => {
                let old = state.applied_revision.resources.get(&effect.resource);
                let new = bundle.resources.get(&effect.resource);
                let old_bindings: BTreeSet<_> = old
                    .and_then(|entry| entry.applies_to.as_ref())
                    .into_iter()
                    .flatten()
                    .collect();
                let new_bindings: BTreeSet<_> = new
                    .and_then(|entry| entry.applies_to.as_ref())
                    .into_iter()
                    .flatten()
                    .collect();
                let source_unchanged = old
                    .zip(new)
                    .is_some_and(|(old, new)| old.digest == new.digest);
                let bindings: BTreeSet<_> = if source_unchanged {
                    old_bindings
                        .symmetric_difference(&new_bindings)
                        .copied()
                        .collect()
                } else {
                    old_bindings.union(&new_bindings).copied().collect()
                };
                for binding in bindings {
                    if let Some(graph) = binding.strip_prefix("graph.") {
                        graphs.insert(graph.to_owned());
                    }
                }
            }
            ResourceKind::EmbeddingProvider(_) => {
                for (address, resource) in state
                    .applied_revision
                    .resources
                    .iter()
                    .chain(bundle.resources.iter())
                {
                    if resource.embedding_provider.as_deref() == Some(&effect.resource) {
                        if let ResourceKind::Graph(graph) = resource_kind(address) {
                            graphs.insert(graph);
                        }
                    }
                }
            }
            ResourceKind::Unknown => {}
        }
    }
    graphs
}

fn source_for<'a>(bundle: &'a DeploymentBundle, address: &str) -> Result<&'a str, Diagnostic> {
    bundle
        .resources
        .get(address)
        .and_then(|resource| bundle.sources.get(&resource.digest))
        .map(String::as_str)
        .ok_or_else(|| {
            refusal(
                "deployment_input_invalid",
                format!("missing captured source for {address}"),
            )
        })
}

async fn open_graph(
    store: &ClusterStore,
    graph: &str,
    policies: &AppliedPolicies,
    read_only: bool,
) -> Result<Omnigraph, Diagnostic> {
    let uri = store.graph_root(graph);
    Omnigraph::ensure_no_pending_recovery(&uri)
        .await
        .map_err(|error| refusal("graph_recovery_required", error.to_string()))?;
    let db = if read_only {
        Omnigraph::open_read_only(&uri).await
    } else if let Some(scope) = store.io_scope() {
        Omnigraph::open_with_io_scope(&uri, scope).await
    } else {
        Omnigraph::open(&uri).await
    }
    .map_err(|error| refusal("graph_unavailable", error.to_string()))?;
    Ok(match policies.graph(graph) {
        Some(policy) => db.with_policy(policy),
        None => db,
    })
}

fn result_from_pending(state: &ClusterState, pending: &OutstandingDeployment) -> DeploymentResult {
    DeploymentResult {
        id: pending.id.clone(),
        input_digest: pending.input_digest.clone(),
        authority: pending.authorization.authority.clone(),
        base: pending.authorization.base.clone(),
        result_revision: state.applied_revision.result_revision.unwrap(),
        config_digest: None,
        graphs: pending
            .graphs
            .iter()
            .map(|(graph, entry)| {
                (
                    graph.clone(),
                    match &entry.state {
                        GraphDeploymentState::Settled { result } => result.as_ref().clone(),
                        _ => GraphDeploymentResult::NotAttempted,
                    },
                )
            })
            .collect(),
        recovery_executors: pending
            .graphs
            .iter()
            .filter_map(|(graph, entry)| {
                entry
                    .recovery_executor
                    .clone()
                    .map(|executor| (graph.clone(), executor))
            })
            .collect(),
        converged: false,
    }
}

pub(crate) fn reserve_completion(
    state: &mut ClusterState,
    bundle: &DeploymentBundle,
) -> Result<(), Diagnostic> {
    let pending = state.outstanding.as_ref().unwrap();
    let counts = |resources: &BTreeMap<String, StateResource>| {
        let mut counts = BTreeMap::<Option<String>, usize>::new();
        for address in resources.keys() {
            let graph = match resource_kind(address) {
                ResourceKind::Graph(graph)
                | ResourceKind::Schema(graph)
                | ResourceKind::Query { graph, .. } => Some(graph),
                _ => None,
            };
            *counts.entry(graph).or_default() += 1;
        }
        counts
    };
    let old_counts = counts(&state.applied_revision.resources);
    let new_counts = counts(&bundle.resources);
    // Each graph's whole schema/query projection advances together. A partial
    // result may keep the larger of its old/new projections independently.
    if old_counts
        .keys()
        .chain(new_counts.keys())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|graph| {
            (*old_counts.get(graph).unwrap_or(&0)).max(*new_counts.get(graph).unwrap_or(&0))
        })
        .sum::<usize>()
        > MAX_RESOURCES
    {
        return Err(refusal(
            "deployment_bounds",
            "partial convergence could exceed the applied resource limit",
        ));
    }
    // Reserve enough counter space for admission, invocation, persisted
    // settlement and terminal recording before allowing the first effect.
    if state
        .state_revision
        .checked_add(
            (pending.graphs.len() as u64)
                .saturating_mul(4)
                .saturating_add(3),
        )
        .is_none()
        || state.applied_revision.result_revision == Some(u64::MAX)
    {
        return Err(refusal(
            "revision_exhausted",
            "deployment cannot reserve its progress revisions",
        ));
    }
    // Each bounded graph outcome contains fixed-width identities, at most three
    // 256-byte actors, one fixed-shape contract and one settlement token. 8 KiB
    // includes worst-case JSON escaping and integer widths. Source-bearing
    // prepared intents are counted from their actual encoding separately.
    let growth = pending
        .graphs
        .len()
        .checked_mul(GRAPH_COMPLETION_RESERVE_BYTES)
        .ok_or_else(|| refusal("deployment_bounds", "completion capacity overflow"))?;
    let result_bytes = encoded_size(&result_from_pending(state, pending))?
        .saturating_add(growth)
        .saturating_add(MAX_DIAGNOSTIC_BYTES);
    if result_bytes > MAX_RESULT_BYTES {
        return Err(refusal(
            "deployment_bounds",
            "deployment result cannot fit its reserved record",
        ));
    }
    state.outstanding.as_mut().unwrap().reserved_result_bytes = result_bytes;
    loop {
        let progress_bytes = encoded_size(state)?
            .saturating_add(growth)
            .saturating_add(128);
        // A terminal partial projection can keep any old graph's resources and
        // adopt any successful graph's desired resources/statuses. Count their
        // union, taking the larger encoding for replaced entries. Query names
        // occur in both the achieved resource map and resource status map.
        let mut terminal = state.clone();
        for (graph, entry) in &state.outstanding.as_ref().unwrap().graphs {
            if let Some(create) = &entry.create {
                terminal
                    .applied_revision
                    .schema_contracts
                    .as_mut()
                    .unwrap()
                    .insert(graph.clone(), create.desired_contract().clone());
            }
        }
        terminal.outstanding = None;
        for (address, desired) in &bundle.resources {
            if terminal
                .applied_revision
                .resources
                .get(address)
                .map(encoded_size)
                .transpose()?
                .unwrap_or(0)
                < encoded_size(desired)?
            {
                terminal
                    .applied_revision
                    .resources
                    .insert(address.clone(), desired.clone());
            }
            let prior_status = terminal.resource_statuses.get(address).cloned();
            set_resource_status_applied(&mut terminal, address);
            if let Some(prior) = prior_status {
                if encoded_size(&prior)? > encoded_size(&terminal.resource_statuses[address])? {
                    terminal.resource_statuses.insert(address.clone(), prior);
                }
            }
        }
        terminal.applied_revision.config_digest = Some(bundle.config_digest.clone());
        let ledger_bytes = progress_bytes.max(
            encoded_size(&terminal)?
                .saturating_add(result_bytes)
                .saturating_add(128),
        );
        let retained = state.deployment_results.as_ref().unwrap();
        if ledger_bytes <= MAX_LEDGER_BYTES
            && retained.len() < MAX_RESULTS
            && encoded_size(retained)?
                .saturating_add(result_bytes)
                .saturating_add(1)
                <= MAX_RESULTS_BYTES
        {
            state.outstanding.as_mut().unwrap().reserved_ledger_bytes = ledger_bytes;
            return Ok(());
        }
        if retained.is_empty() {
            return Err(refusal(
                "deployment_bounds",
                "deployment cannot reserve ledger completion capacity",
            ));
        }
        state.deployment_results.as_mut().unwrap().remove(0);
    }
}

/// Capture all source bytes once, resolving storage identity on this host.
pub fn capture_deployment(config_dir: impl AsRef<Path>) -> Result<CapturedDeployment, Diagnostic> {
    capture_deployment_input(config_dir.as_ref(), None)
}

/// Capture local sources for the root advertised by an authenticated server.
/// No storage backend is constructed and no server path is opened on this host.
/// Omitted storage selects that server; an explicit storage root must match its
/// canonical spelling without guessing symlinks or resolving relative paths.
/// The receiving executor still verifies canonical identity on the server.
pub fn capture_deployment_for_server(
    config_dir: impl AsRef<Path>,
    server_canonical_root: &str,
) -> Result<CapturedDeployment, Diagnostic> {
    if remote_root_identity(server_canonical_root)? != server_canonical_root {
        return Err(refusal(
            "storage_root_invalid",
            "the server must advertise a canonical storage root",
        ));
    }
    capture_deployment_input(config_dir.as_ref(), Some(server_canonical_root))
}

fn remote_root_identity(root: &str) -> Result<String, Diagnostic> {
    let normalized = omnigraph_storage::normalize_root_uri(root)
        .map_err(|error| refusal("storage_root_invalid", error.to_string()))?;
    match omnigraph_storage::storage_kind_for_uri(&normalized)
        .map_err(|error| refusal("storage_root_invalid", error.to_string()))?
    {
        omnigraph_storage::StorageKind::Local => {
            if !Path::new(&normalized).is_absolute() {
                return Err(refusal(
                    "storage_root_invalid",
                    "remote deployment requires an absolute storage root; omit storage to select the server's root",
                ));
            }
            Ok(format!("file://{normalized}"))
        }
        omnigraph_storage::StorageKind::S3 | omnigraph_storage::StorageKind::Azure => {
            Ok(normalized)
        }
    }
}

fn capture_deployment_input(
    config_dir: &Path,
    server_canonical_root: Option<&str>,
) -> Result<CapturedDeployment, Diagnostic> {
    let captured = config::capture_desired(config_dir);
    if let Some(error) = captured
        .outcome
        .diagnostics
        .into_iter()
        .find(|error| error.severity == DiagnosticSeverity::Error)
    {
        return Err(error);
    }
    let desired = captured
        .outcome
        .desired
        .ok_or_else(|| refusal("configuration_invalid", "configuration unavailable"))?;
    capture_desired_deployment(&desired, captured.sources, server_canonical_root)
}

pub(crate) fn capture_desired_deployment(
    desired: &DesiredCluster,
    sources: BTreeMap<String, std::sync::Arc<str>>,
    server_canonical_root: Option<&str>,
) -> Result<CapturedDeployment, Diagnostic> {
    if !desired.state_lock {
        return Err(refusal(
            "deployment_requires_lock",
            "deployment requires cluster admission",
        ));
    }
    let root = match server_canonical_root {
        Some(root) => root.to_owned(),
        None => {
            store_for(&desired.config_dir, desired.storage_root.as_deref())?.canonical_root()?
        }
    };
    let bundle = bundle_from_capture(desired, sources, root.clone());
    if server_canonical_root.is_some() {
        validate_bundle_with_root(&bundle, &root, remote_root_identity)?;
    } else {
        validate_bundle(&bundle, &root)?;
    }
    Ok(bundle)
}

fn verify_recorded_input(
    state: &ClusterState,
    id: &str,
    bundle: &CapturedDeployment,
) -> Result<(), Diagnostic> {
    let digest = bundle.input_digest()?;
    let recorded = state
        .outstanding
        .as_ref()
        .filter(|entry| entry.id == id)
        .map(|entry| &entry.input_digest)
        .or_else(|| {
            state
                .deployment_results
                .as_ref()
                .unwrap()
                .iter()
                .find(|entry| entry.id == id)
                .map(|entry| &entry.input_digest)
        });
    if recorded.is_some_and(|recorded| recorded != &digest) {
        return Err(refusal(
            "deployment_input_mismatch",
            "original deployment identity has different immutable input",
        ));
    }
    Ok(())
}

/// Runtime preview derived from captured control metadata, without graph opens
/// or schema-gate acquisition. Full engine preparation follows request drainage.
pub struct DeploymentPreview {
    pub affected_graphs: Vec<String>,
    pub serving: ServingSnapshot,
}

pub async fn prepare_deployment_preview(
    bundle: &CapturedDeployment,
    admission: &ClusterAdmission,
    caller: &DeploymentCaller,
) -> Result<DeploymentPreview, Diagnostic> {
    let (store, state, cas, affected_graphs) =
        capture_deployment_preview(bundle, admission, caller).await?;
    let serving = serve::preview_snapshot_with_store(&store, bundle, &affected_graphs, state, cas)
        .await
        .map_err(|mut diagnostics| diagnostics.remove(0))?;
    Ok(DeploymentPreview {
        affected_graphs,
        serving,
    })
}

async fn capture_deployment_preview(
    bundle: &CapturedDeployment,
    admission: &ClusterAdmission,
    caller: &DeploymentCaller,
) -> Result<(ClusterStore, ClusterState, String, Vec<String>), Diagnostic> {
    if bundle.canonical_root() != admission.canonical_root() {
        return Err(refusal(
            "cluster_admission_root_mismatch",
            "deployment root differs from writer admission",
        ));
    }
    validate_bundle(bundle, admission.canonical_root())?;
    admission.validate_deployment().await?;
    let store = ClusterStore::for_storage_root(admission.canonical_root())?;
    let (state, cas) = read_existing(&store).await?;
    require_v2(&state)?;
    let policy = deployment_policies(&store, &state, bundle, caller).await?;
    if state.outstanding.is_some() {
        return Err(refusal(
            "cluster_deployment_outstanding",
            "original deployment remains outstanding",
        ));
    }
    authorization::refuse_pending_recovery(&store).await?;
    let effects = preview_deployment_scope(&state, bundle)?;
    let affected_graphs: Vec<_> = affected_graphs(&state, bundle, &effects)
        .into_iter()
        .collect();
    for graph in &affected_graphs {
        authorize_graph_change(&state, bundle, caller, &policy, graph)?;
    }
    Ok((store, state, cas, affected_graphs))
}

/// Observe a captured deployment through the serving owner's existing handles.
/// No graph is opened, no admission is closed, and no execution intent or
/// sequence is reserved. Apply repeats physical eligibility after drainage.
pub async fn plan_captured_deployment(
    bundle: &CapturedDeployment,
    admission: &ClusterAdmission,
    caller: &DeploymentCaller,
    live_graphs: &BTreeMap<String, std::sync::Arc<Omnigraph>>,
) -> Result<PlanOutput, Diagnostic> {
    let (store, state, cas, affected) =
        capture_deployment_preview(bundle, admission, caller).await?;
    // Verify the same control payloads and candidate bindings used by preview.
    // This projection reads no graph datasets or source paths on the client.
    serve::preview_snapshot_with_store(&store, bundle, &affected, state.clone(), cas.clone())
        .await
        .map_err(|mut diagnostics| diagnostics.remove(0))?;
    let mut changes =
        crate::diff::diff_state_resources(&state.applied_revision.resources, &bundle.resources);
    let mut diagnostics = Vec::new();
    for graph in &affected {
        if !state
            .applied_revision
            .resources
            .contains_key(&graph_address(graph))
        {
            continue;
        }
        let Some(db) = live_graphs.get(graph) else {
            if !bundle.resources.contains_key(&graph_address(graph)) {
                // Deletion can remove a blocked or missing graph. Its ledger
                // identity is enough to describe the change; apply verifies
                // the physical root after draining affected work.
                continue;
            }
            return Err(refusal(
                "graph_unavailable",
                format!("graph {graph} has no ready serving handle for an observed plan"),
            ));
        };
        admission::admitted_graph_id(
            &bundle.canonical_root,
            state.applied_revision.schema_contracts.as_ref().unwrap(),
            db.uri(),
        )?;
        let expected = &state.applied_revision.schema_contracts.as_ref().unwrap()[graph];
        if &db.schema_contract_digest() != expected {
            return Err(refusal(
                "applied_schema_drift",
                format!("graph {graph} differs from its achieved contract"),
            ));
        }
        let address = schema_address(graph);
        if let Some(change) = changes
            .iter_mut()
            .find(|change| change.resource == address && change.operation == PlanOperation::Update)
        {
            let migration = db
                .plan_schema_at_contract(source_for(bundle, &address)?, expected)
                .map_err(|error| refusal("schema_preview_unavailable", error.to_string()))?;
            if !migration.supported {
                diagnostics.push(refusal(
                    "schema_preflight_failed",
                    "observed schema migration is unsupported",
                ));
            }
            if db
                .branch_list()
                .await
                .map_err(|error| refusal("schema_preview_unavailable", error.to_string()))?
                .iter()
                .any(|branch| branch != "main")
            {
                diagnostics.push(refusal(
                    "schema_preflight_failed",
                    format!("graph {graph} schema apply requires only main"),
                ));
            }
            change.migration = Some(migration);
        }
    }
    annotate_plan_changes(&store, &mut changes, diagnostics.first())?;
    let mut dependencies = BTreeSet::new();
    for (address, resource) in &bundle.resources {
        match resource_kind(address) {
            ResourceKind::Schema(graph) => {
                dependencies.insert(Dependency {
                    from: address.clone(),
                    to: graph_address(&graph),
                });
            }
            ResourceKind::Query { graph, .. } => {
                dependencies.insert(Dependency {
                    from: address.clone(),
                    to: graph_address(&graph),
                });
                dependencies.insert(Dependency {
                    from: address.clone(),
                    to: schema_address(&graph),
                });
            }
            ResourceKind::Policy(_) => {
                for target in resource
                    .applies_to
                    .iter()
                    .flatten()
                    .filter(|target| target.starts_with("graph."))
                {
                    dependencies.insert(Dependency {
                        from: address.clone(),
                        to: target.clone(),
                    });
                }
            }
            ResourceKind::Graph(_) => {
                if let Some(provider) = &resource.embedding_provider {
                    dependencies.insert(Dependency {
                        from: address.clone(),
                        to: provider.clone(),
                    });
                }
            }
            _ => {}
        }
    }
    let dependencies: Vec<_> = dependencies.into_iter().collect();
    let mut observations = store.observations();
    observations.state_found = true;
    observations.state_revision = state.state_revision;
    observations.state_cas = Some(cas);
    observations.applied_config_digest = state.applied_revision.config_digest;
    observations.resource_count = state.applied_revision.resources.len();
    // The lifetime admission is already held. A plan does not acquire it.
    observations.locked = true;
    observations.lock_id = Some(admission.lock_id().to_owned());
    Ok(PlanOutput {
        ok: !has_errors(&diagnostics),
        authority: LedgerAuthority::Observed,
        config_dir: bundle.canonical_root.clone(),
        desired_revision: DesiredRevision {
            config_digest: Some(bundle.config_digest.clone()),
        },
        input_digest: Some(bundle.input_digest()?),
        resource_digests: bundle
            .resources
            .iter()
            .map(|(address, resource)| (address.clone(), resource.digest.clone()))
            .collect(),
        blast_radius: compute_blast_radius(&changes, &dependencies),
        dependencies,
        state_observations: observations,
        changes,
        diagnostics,
    })
}

pub(crate) fn annotate_plan_changes(
    store: &ClusterStore,
    changes: &mut [PlanChange],
    error: Option<&Diagnostic>,
) -> Result<(), Diagnostic> {
    for change in changes {
        change.disposition = Some(if error.is_some() {
            ApplyDisposition::Blocked
        } else if matches!(resource_kind(&change.resource), ResourceKind::Graph(_))
            && change.operation == PlanOperation::Update
        {
            ApplyDisposition::Derived
        } else {
            ApplyDisposition::Applied
        });
        change.reason = error.map(|error| error.code.clone());
        if let ResourceKind::Graph(graph) = resource_kind(&change.resource)
            && change.operation == PlanOperation::Delete
        {
            change.delete_root = Some(store.canonical_managed_graph_root(&graph)?);
        }
    }
    Ok(())
}

/// Authorize from applied control metadata before any graph gate or open.
/// The executor repeats the same check at its fresh accepted-base capture.
fn authorize_graph_change(
    state: &ClusterState,
    bundle: &DeploymentBundle,
    caller: &DeploymentCaller,
    policy: &AppliedPolicies,
    graph: &str,
) -> Result<(), Diagnostic> {
    if !state
        .applied_revision
        .resources
        .contains_key(&graph_address(graph))
    {
        return Ok(());
    }
    if !bundle.resources.contains_key(&graph_address(graph)) {
        return authorize_graph_deletion(policy, caller, graph);
    }
    let schema = schema_address(graph);
    if state.applied_revision.resources[&schema].digest != bundle.resources[&schema].digest
        && let DeploymentCaller::AuthenticatedIdentity(identity) = caller
    {
        policy.check_graph(identity.actor(), graph, PolicyAction::Read)?;
        policy.check_graph(identity.actor(), graph, PolicyAction::SchemaApply)?;
    }
    Ok(())
}

/// Submit a frozen deployment under direct writer admission. `report_id` runs before acceptance and
/// graph effects, so a severed caller can later look up the original identity.
/// Resubmission with an existing ID only observes; it never executes again.
pub async fn apply_deployment(
    config_dir: impl AsRef<Path>,
    requested_id: Option<&str>,
    caller: &DeploymentCaller,
    report_id: impl FnOnce(&str, &str, &str),
) -> Result<DeploymentLookup, Diagnostic> {
    let bundle = capture_deployment(config_dir)?;
    let store = ClusterStore::for_storage_root(bundle.canonical_root())?;
    bootstrap_ledger(&store, &bundle, caller).await?;
    if let Some(id) = requested_id {
        let (state, _) = read_existing(&store).await?;
        let found = lookup(&state, id)?;
        if !matches!(found, DeploymentLookup::NotRecorded) {
            lookup_policies(&store, &state, caller, Some(id)).await?;
            verify_recorded_input(&state, id, &bundle)?;
            return Ok(found);
        }
    }
    let admission = admission::acquire_with_store(&store, ClusterAdmissionPurpose::Deployment)
        .await?
        .ok_or_else(|| refusal("state_missing", "cluster ledger is missing"))?;
    let mut effects_started = false;
    let result = execute_captured_deployment(
        &bundle,
        requested_id,
        caller,
        &admission,
        &BTreeMap::new(),
        report_id,
        |_| {},
        &mut effects_started,
    )
    .await;
    if !effects_started {
        return match result {
            Err(error) => Err(admission.release_refused_preflight(error).await),
            Ok(result) => {
                admission.release_after_settlement().await?;
                Ok(result)
            }
        };
    }
    result
}

/// Execute under the existing sole writer. The server supplies its paused live
/// handles; direct execution opens graphs under the same root admission.
/// Every affected live handle must be drained with healthy native operations
/// before invocation. Root admission excludes participating writers; it does
/// not fence excluded/raw writers or prove remote I/O settlement after failure.
/// `on_accepted` receives the confirmed outstanding receipt after its ledger
/// CAS, before graph effects. Lookup and pre-acceptance refusal never call it.
#[allow(clippy::too_many_arguments)] // Distinct identity-report and durable-acceptance boundaries.
pub async fn apply_captured_deployment(
    bundle: &CapturedDeployment,
    requested_id: Option<&str>,
    caller: &DeploymentCaller,
    admission: &ClusterAdmission,
    live_graphs: &BTreeMap<String, std::sync::Arc<Omnigraph>>,
    report_id: impl FnOnce(&str, &str, &str),
    on_accepted: impl FnOnce(DeploymentLookup),
    effects_started: &mut bool,
) -> Result<DeploymentLookup, Diagnostic> {
    execute_captured_deployment(
        bundle,
        requested_id,
        caller,
        admission,
        live_graphs,
        report_id,
        on_accepted,
        effects_started,
    )
    .await
}

#[allow(clippy::too_many_arguments)] // Shared executor for direct and served acceptance observers.
async fn execute_captured_deployment(
    bundle: &CapturedDeployment,
    requested_id: Option<&str>,
    caller: &DeploymentCaller,
    admission: &ClusterAdmission,
    live_graphs: &BTreeMap<String, std::sync::Arc<Omnigraph>>,
    report_id: impl FnOnce(&str, &str, &str),
    on_accepted: impl FnOnce(DeploymentLookup),
    effects_started: &mut bool,
) -> Result<DeploymentLookup, Diagnostic> {
    let store = admission.store();
    execute_captured_deployment_in_store(
        &store,
        bundle,
        requested_id,
        caller,
        admission,
        live_graphs,
        report_id,
        on_accepted,
        effects_started,
    )
    .await
}

#[allow(clippy::too_many_arguments)] // Same executor under the private bootstrap store/owner.
pub(super) async fn execute_captured_deployment_in_store(
    store: &ClusterStore,
    bundle: &CapturedDeployment,
    requested_id: Option<&str>,
    caller: &DeploymentCaller,
    admission: &ClusterAdmission,
    live_graphs: &BTreeMap<String, std::sync::Arc<Omnigraph>>,
    report_id: impl FnOnce(&str, &str, &str),
    on_accepted: impl FnOnce(DeploymentLookup),
    effects_started: &mut bool,
) -> Result<DeploymentLookup, Diagnostic> {
    let root = store.canonical_root()?;
    validate_bundle(bundle, &root)?;
    if admission.canonical_root() != root {
        return Err(refusal(
            "cluster_admission_root_mismatch",
            "deployment root differs from writer admission",
        ));
    }
    admission.validate_deployment().await?;
    let input_digest = bundle.input_digest()?;
    // Existing identities are lookup-only, including while their owner holds
    // the lock. Input equality is required before exposing the original result.
    let (before, before_cas) = read_existing(store).await?;
    require_v2(&before)?;
    if let Some(id) = requested_id {
        let existing = lookup(&before, id)?;
        if !matches!(existing, DeploymentLookup::NotRecorded) {
            lookup_policies(store, &before, caller, Some(id)).await?;
            let recorded_digest = before
                .outstanding
                .as_ref()
                .filter(|entry| entry.id == id)
                .map(|entry| entry.input_digest.as_str())
                .or_else(|| {
                    before
                        .deployment_results
                        .as_ref()
                        .unwrap()
                        .iter()
                        .find(|entry| entry.id == id)
                        .map(|entry| entry.input_digest.as_str())
                });
            if recorded_digest.is_some_and(|digest| digest != input_digest) {
                return Err(refusal(
                    "deployment_input_mismatch",
                    "original deployment identity has different immutable input",
                ));
            }
            return Ok(existing);
        }
    }
    deployment_policies(store, &before, bundle, caller).await?;
    let sequence = before.next_sequence.unwrap();
    let id = requested_id.map(str::to_owned).unwrap_or_else(|| {
        format!(
            "{}:{sequence}:{}",
            before.ledger_id.as_ref().unwrap(),
            Ulid::new()
        )
    });
    let parsed_id = DeploymentId::parse(&id)?;
    if Some(parsed_id.ledger.as_str()) != before.ledger_id.as_deref()
        || parsed_id.sequence != sequence
    {
        return Err(refusal(
            "deployment_id_stale",
            "new deployment must use this ledger's exact next sequence",
        ));
    }
    report_id(&id, admission.canonical_root(), admission.lock_id());
    let PreparedDeployment {
        mut state,
        mut cas,
        policy,
        ..
    } = match prepare_deployment(
        store,
        bundle,
        caller,
        (before, before_cas),
        live_graphs,
        &id,
        &input_digest,
    )
    .await
    {
        Ok(prepared) => prepared,
        Err(error) => return Err(error),
    };
    // From here immutable control writes and writable engine opens may start.
    // Neither an error nor cancellation is a settlement proof for this phase.
    *effects_started = true;
    if store.write_deployment_bundle(bundle).await? != input_digest {
        return Err(refusal(
            "deployment_input_invalid",
            "bundle encoding identity changed",
        ));
    }
    seams::fail(&DEPLOYMENT_BEFORE_ACCEPTANCE)?;
    cas = replace(store, &mut state, &cas).await?;
    on_accepted(lookup(&state, &id)?);
    seams::fail(&DEPLOYMENT_AFTER_ACCEPTANCE)?;
    install_catalog_payloads(store, &state, bundle).await?;
    for (graph, entry) in state.outstanding.as_ref().unwrap().graphs.clone() {
        let result = if let Some(delete) = &entry.delete {
            state
                .outstanding
                .as_mut()
                .unwrap()
                .graphs
                .get_mut(&graph)
                .unwrap()
                .state = GraphDeploymentState::Started;
            cas = replace(store, &mut state, &cas).await?;
            seams::fail(&DEPLOYMENT_AFTER_STARTED)?;
            complete_graph_deletion(store, &graph, delete, &id, admission).await?;
            seams::fail(&DEPLOYMENT_AFTER_SCHEMA)?;
            GraphDeploymentResult::Deleted {
                contract: delete.contract.clone(),
            }
        } else if let Some(create) = &entry.create {
            state
                .outstanding
                .as_mut()
                .unwrap()
                .graphs
                .get_mut(&graph)
                .unwrap()
                .state = GraphDeploymentState::Started;
            cas = replace(store, &mut state, &cas).await?;
            seams::fail(&DEPLOYMENT_AFTER_STARTED)?;
            let db = Omnigraph::apply_prepared_graph_create_with_io_scope(create, store.io_scope()).await
                .map_err(|error| refusal("deployment_outcome_unknown", format!("deployment {id} graph creation remains outstanding under admission {}: {error}", admission.lock_id())))?;
            let snapshot = db
                .snapshot_of(ReadTarget::branch("main"))
                .await
                .map_err(|error| refusal("deployment_outcome_unknown", error.to_string()))?;
            seams::fail(&DEPLOYMENT_AFTER_SCHEMA)?;
            GraphDeploymentResult::Created {
                graph_manifest_version: snapshot.graph_manifest_version(),
                contract: db.schema_contract_digest(),
            }
        } else if let Some(intent) = &entry.intent {
            let db = match live_graphs.get(&graph) {
                Some(db) => db.clone(),
                None => std::sync::Arc::new(
                    open_graph(store, &graph, &policy, false)
                        .await
                        .map_err(|error| retained_error(error, admission.lock_id()))?,
                ),
            };
            state
                .outstanding
                .as_mut()
                .unwrap()
                .graphs
                .get_mut(&graph)
                .unwrap()
                .state = GraphDeploymentState::Started;
            cas = replace(store, &mut state, &cas).await?;
            seams::fail(&DEPLOYMENT_AFTER_STARTED)?;
            let applied = db.apply_prepared_schema_as(intent, caller.actor()).await
                .map_err(|error| refusal("deployment_outcome_unknown", format!("deployment {id} remains outstanding under admission {}; schema invocation failed: {error}; establish quiescence and reconcile its original identity", admission.lock_id())))?;
            seams::fail(&DEPLOYMENT_AFTER_SCHEMA)?;
            GraphDeploymentResult::Schema {
                result: match applied.commit {
                    Some(commit) => SchemaApplySettlement::Committed {
                        commit,
                        contract: applied.contract,
                    },
                    None => SchemaApplySettlement::NoOp {
                        graph_manifest_version: applied.graph_manifest_version,
                        head_commit_id: intent.base_head_commit_id().map(str::to_owned),
                        contract: applied.contract,
                    },
                },
            }
        } else {
            GraphDeploymentResult::QueryOnly {
                graph_manifest_version: entry.observed_manifest_version,
                schema_digest: bundle.resources[&schema_address(&graph)].digest.clone(),
            }
        };
        state
            .outstanding
            .as_mut()
            .unwrap()
            .graphs
            .get_mut(&graph)
            .unwrap()
            .state = GraphDeploymentState::Settled {
            result: Box::new(result),
        };
        cas = replace(store, &mut state, &cas).await?;
    }
    let result = finish(store, &mut state, &cas, bundle).await?;
    // Exact publication is not a proof that all accepted native/control I/O
    // has stopped. Retain admission until explicit operator quiescence/unlock.
    Ok(DeploymentLookup::Complete { result })
}

struct PreparedDeployment {
    state: ClusterState,
    cas: String,
    policy: AppliedPolicies,
    migrations: BTreeMap<String, SchemaMigrationPlan>,
}

/// Awaited read-only preflight. Engine preparation only reads accepted
/// authority and plans the migration; all opened handles are read-only and
/// dropped before this returns. No payload, ledger or native write is issued.
async fn prepare_deployment(
    store: &ClusterStore,
    bundle: &DeploymentBundle,
    caller: &DeploymentCaller,
    captured_state: (ClusterState, String),
    live_graphs: &BTreeMap<String, std::sync::Arc<Omnigraph>>,
    id: &str,
    input_digest: &str,
) -> Result<PreparedDeployment, Diagnostic> {
    let (mut state, cas) = captured_state;
    require_v2(&state)?;
    let policy = deployment_policies(store, &state, bundle, caller).await?;
    if state.outstanding.is_some() {
        return Err(refusal(
            "cluster_deployment_outstanding",
            "original deployment remains outstanding",
        ));
    }
    authorization::refuse_pending_recovery(store).await?;
    let effects = preview_deployment_scope(&state, bundle)?;
    let sequence = state.next_sequence.unwrap();
    let next = sequence
        .checked_add(1)
        .ok_or_else(|| refusal("sequence_exhausted", "deployment sequence exhausted"))?;
    let parsed_id = DeploymentId::parse(id)?;
    if Some(parsed_id.ledger.as_str()) != state.ledger_id.as_deref()
        || parsed_id.sequence != sequence
    {
        return Err(refusal(
            "deployment_id_stale",
            "new deployment must use this ledger's exact next sequence",
        ));
    }
    let (graphs, migrations) = Box::pin(prepare_graphs(
        store,
        &state,
        bundle,
        caller,
        &policy,
        live_graphs,
        &effects,
    ))
    .await?;
    let authorization = DeploymentAuthorization {
        version: 2,
        ledger_id: state.ledger_id.clone().unwrap(),
        authority: caller.authority()?,
        base: AchievedDeploymentBase {
            result_revision: state.applied_revision.result_revision.unwrap(),
            resource_digests: state_resource_digests(&state),
            schema_contracts: state.applied_revision.schema_contracts.clone().unwrap(),
            capture_cas: cas.clone(),
        },
        input_digest: input_digest.to_owned(),
        policy_digests: policy.digests.clone(),
        effects,
    };
    state.next_sequence = Some(next);
    state.outstanding = Some(OutstandingDeployment {
        id: id.to_owned(),
        input_digest: input_digest.to_owned(),
        authorization,
        graphs,
        reserved_ledger_bytes: 0,
        reserved_result_bytes: 0,
    });
    reserve_completion(&mut state, bundle)?;
    validate_state(&state)?;
    Ok(PreparedDeployment {
        state,
        cas,
        policy,
        migrations,
    })
}

/// Full read-only deployment preflight. It acquires no writer admission and
/// never grants authority to execute; apply repeats it under the sole writer.
pub async fn preflight_deployment(
    bundle: &CapturedDeployment,
    caller: &DeploymentCaller,
) -> Result<BTreeMap<String, SchemaMigrationPlan>, Diagnostic> {
    let store = ClusterStore::for_storage_root(bundle.canonical_root())?;
    validate_bundle(bundle, &store.canonical_root()?)?;
    let snapshot = store.read_state(&mut store.observations()).await?;
    preflight_deployment_at(bundle, caller, snapshot).await
}

pub(crate) async fn preflight_deployment_at(
    bundle: &CapturedDeployment,
    caller: &DeploymentCaller,
    snapshot: crate::store::StateSnapshot,
) -> Result<BTreeMap<String, SchemaMigrationPlan>, Diagnostic> {
    let store = ClusterStore::for_storage_root(bundle.canonical_root())?;
    validate_bundle(bundle, &store.canonical_root()?)?;
    let state = snapshot.state.unwrap_or_else(empty_ledger);
    require_v2(&state)?;
    let id = format!(
        "{}:{}:{}",
        state.ledger_id.as_ref().unwrap(),
        state.next_sequence.unwrap(),
        Ulid::new()
    );
    let cas = snapshot
        .state_cas
        .unwrap_or_else(|| format!("sha256:{}", sha256_hex(b"")));
    let input_digest = bundle.input_digest()?;
    let prepared = prepare_deployment(
        &store,
        bundle,
        caller,
        (state, cas),
        &BTreeMap::new(),
        &id,
        &input_digest,
    )
    .await?;

    Ok(prepared.migrations)
}

async fn prepare_graphs(
    store: &ClusterStore,
    state: &ClusterState,
    bundle: &DeploymentBundle,
    caller: &DeploymentCaller,
    policy: &AppliedPolicies,
    live_graphs: &BTreeMap<String, std::sync::Arc<Omnigraph>>,
    effects: &[AuthorizedEffect],
) -> Result<
    (
        BTreeMap<String, GraphDeployment>,
        BTreeMap<String, SchemaMigrationPlan>,
    ),
    Diagnostic,
> {
    let affected = affected_graphs(state, bundle, effects);
    // Validate the applied payload even when this deployment replaces or removes
    // it. New desired bytes cannot grant an implicit catalog-repair capability.
    let mut checked = BTreeSet::new();
    for (address, resource, applied) in state
        .applied_revision
        .resources
        .iter()
        .map(|(address, resource)| (address, resource, true))
        .chain(
            bundle
                .resources
                .iter()
                .map(|(address, resource)| (address, resource, false)),
        )
    {
        let kind = resource_kind(address);
        let relevant = match &kind {
            ResourceKind::Query { graph, .. } => affected.contains(graph),
            ResourceKind::Policy(_) => true,
            _ => false,
        };
        if !relevant || !checked.insert((address, &resource.digest)) {
            continue;
        }
        let prior = store
            .read_payload(&kind, &resource.digest)
            .await
            .map_err(|error| refusal("resource_payload_read_error", error))?;
        let invalid = prior
            .as_ref()
            .is_some_and(|source| sha256_hex(source.as_bytes()) != resource.digest)
            || (prior.is_none() && applied);
        if invalid {
            return Err(refusal(
                "catalog_payload_invalid",
                format!(
                    "{address} is missing or corrupt; restore its exact recorded payload before deployment"
                ),
            ));
        }
    }
    let mut graphs = BTreeMap::new();
    let mut migrations = BTreeMap::new();
    for graph in affected {
        authorize_graph_change(state, bundle, caller, policy, &graph)?;
        let existing = state
            .applied_revision
            .resources
            .contains_key(&graph_address(&graph));
        let schema_address = schema_address(&graph);
        let mut entry = GraphDeployment {
            create: None,
            delete: None,
            intent: None,
            observed_manifest_version: 0,
            state: GraphDeploymentState::NotStarted,
            settlement: None,
            recovery_executor: None,
        };
        if !existing {
            let uri = store.graph_root(&graph);
            let canonical = admission::canonical_graph_uri(&uri)?;
            let expected_uri = format!(
                "{}/graphs/{graph}.omni",
                bundle.canonical_root.trim_start_matches("file://")
            );
            if canonical != expected_uri {
                return Err(refusal(
                    "cluster_graph_root_mismatch",
                    "graph root is outside the admitted cluster's canonical graph layout",
                ));
            }
            if store
                .graph_root_exists(&uri)
                .await
                .map_err(|error| refusal("graph_unavailable", error.to_string()))?
                && !store
                    .graph_root_is_empty_local_directory(&uri)
                    .map_err(|error| refusal("graph_unavailable", error.to_string()))?
            {
                return Err(refusal(
                    "graph_root_exists",
                    format!(
                        "graph {graph} already has unmanaged storage; ordinary deployment never adopts or overwrites it"
                    ),
                ));
            }
            let create =
                Omnigraph::prepare_graph_create(&uri, source_for(bundle, &schema_address)?)
                    .await
                    .map_err(|error| refusal("graph_create_preflight_failed", error.to_string()))?;
            admission::admitted_graph_id(
                &bundle.canonical_root,
                &BTreeMap::from([(graph.clone(), create.desired_contract().clone())]),
                create.root(),
            )?;
            entry.create = Some(create);
            graphs.insert(graph, entry);
            continue;
        }
        if !bundle.resources.contains_key(&graph_address(&graph)) {
            let root = store.canonical_managed_graph_root(&graph)?;
            let contract =
                state.applied_revision.schema_contracts.as_ref().unwrap()[&graph].clone();
            if store
                .graph_root_exists(&root)
                .await
                .map_err(|error| refusal("graph_unavailable", error.to_string()))?
            {
                let db = match live_graphs.get(&graph) {
                    Some(db) => db.clone(),
                    None => std::sync::Arc::new(open_graph(store, &graph, policy, true).await?),
                };
                admission::admitted_graph_id(
                    &bundle.canonical_root,
                    state.applied_revision.schema_contracts.as_ref().unwrap(),
                    db.uri(),
                )?;
                if db.schema_contract_digest() != contract {
                    return Err(refusal(
                        "applied_schema_drift",
                        format!("graph {graph} differs from the managed identity being deleted"),
                    ));
                }
                entry.observed_manifest_version = db
                    .snapshot_of(ReadTarget::branch("main"))
                    .await
                    .map_err(|error| refusal("graph_unavailable", error.to_string()))?
                    .graph_manifest_version();
            }
            entry.delete = Some(GraphDeletion { root, contract });
            graphs.insert(graph, entry);
            continue;
        }
        let schema_changed = state.applied_revision.resources[&schema_address].digest
            != bundle.resources[&schema_address].digest;
        let db = match live_graphs.get(&graph) {
            Some(db) => db.clone(),
            None => std::sync::Arc::new(open_graph(store, &graph, policy, true).await?),
        };
        admission::admitted_graph_id(
            &bundle.canonical_root,
            state.applied_revision.schema_contracts.as_ref().unwrap(),
            db.uri(),
        )?;
        let snapshot = db
            .snapshot_of(ReadTarget::branch("main"))
            .await
            .map_err(|error| refusal("graph_unavailable", error.to_string()))?;
        let observed = db.schema_contract_digest();
        let achieved = &state.applied_revision.schema_contracts.as_ref().unwrap()[&graph];
        if &observed != achieved {
            return Err(refusal(
                "applied_schema_drift",
                format!(
                    "graph {graph} differs from its achieved contract; restore the recorded graph before deployment"
                ),
            ));
        }
        if schema_changed {
            let (intent, migration) = db
                .prepare_schema_apply_with_plan_as(
                    source_for(bundle, &schema_address)?,
                    caller.actor(),
                )
                .await
                .map_err(|error| refusal("schema_preflight_failed", error.to_string()))?;
            if intent.base_contract() != achieved {
                return Err(refusal(
                    "applied_schema_drift",
                    format!("graph {graph} changed during schema preparation"),
                ));
            }
            entry.observed_manifest_version = intent.base_manifest_version();
            entry.intent = Some(intent);
            migrations.insert(graph.clone(), migration);
        } else {
            entry.observed_manifest_version = snapshot.graph_manifest_version();
        }
        graphs.insert(graph, entry);
    }
    Ok((graphs, migrations))
}

fn authorize_graph_deletion(
    policy: &AppliedPolicies,
    caller: &DeploymentCaller,
    graph: &str,
) -> Result<(), Diagnostic> {
    if matches!(caller, DeploymentCaller::AuthenticatedIdentity(_)) || policy.graph(graph).is_some()
    {
        let actor = caller.actor().ok_or_else(|| {
            refusal(
                "policy_denied",
                "graph deletion requires an actor when policy is installed",
            )
        })?;
        policy.check_graph(actor, graph, PolicyAction::Read)?;
        policy.check_graph(actor, graph, PolicyAction::SchemaApply)?;
    }
    Ok(())
}

/// The caller owns the cluster writer and has drained all affected work. The
/// durable Started record is the completion authority if prefix deletion stops
/// after removing only part of the root.
async fn complete_graph_deletion(
    store: &ClusterStore,
    graph: &str,
    delete: &GraphDeletion,
    deployment_id: &str,
    admission: &ClusterAdmission,
) -> Result<(), Diagnostic> {
    admission.validate_completion(deployment_id).await?;
    admission::admitted_graph_id(
        admission.canonical_root(),
        &BTreeMap::from([(graph.to_owned(), delete.contract.clone())]),
        &delete.root,
    )?;
    store
        .delete_managed_graph_root(graph, &delete.root)
        .await
        .map_err(|error| retained_error(error, admission.lock_id()))
}

async fn install_catalog_payloads(
    store: &ClusterStore,
    state: &ClusterState,
    bundle: &DeploymentBundle,
) -> Result<(), Diagnostic> {
    for (address, resource) in &bundle.resources {
        let kind = resource_kind(address);
        if matches!(kind, ResourceKind::Query { .. } | ResourceKind::Policy(_)) {
            if state
                .applied_revision
                .resources
                .get(address)
                .is_some_and(|old| old.digest == resource.digest)
            {
                continue;
            }
            let source = source_for(bundle, address)?;
            store
                .write_payload(&kind, &resource.digest, source)
                .await
                .map_err(|error| refusal("resource_payload_write_error", error))?;
        }
    }
    Ok(())
}

fn retained_error(error: Diagnostic, lock_id: &str) -> Diagnostic {
    refusal(
        &error.code,
        format!(
            "admission {lock_id} remains held pending settlement; {}",
            error.message
        ),
    )
}

fn validate_pending_input(
    state: &ClusterState,
    pending: &OutstandingDeployment,
    bundle: &DeploymentBundle,
) -> Result<(), Diagnostic> {
    let effects = preview_deployment_scope(state, bundle)?;
    let affected = affected_graphs(state, bundle, &effects);
    if effects != pending.authorization.effects
        || affected != pending.graphs.keys().cloned().collect()
    {
        return Err(refusal(
            "deployment_authority_changed",
            "stored graph effects differ from immutable input",
        ));
    }
    for (graph, entry) in &pending.graphs {
        let address = schema_address(graph);
        if let Some(create) = &entry.create {
            admission::admitted_graph_id(
                &bundle.canonical_root,
                &BTreeMap::from([(graph.clone(), create.desired_contract().clone())]),
                create.root(),
            )?;
            let expected_root = admission::canonical_graph_uri(&format!(
                "{}/graphs/{graph}.omni",
                bundle.canonical_root.trim_end_matches('/')
            ))?;
            if create.root() != expected_root
                || create.desired_contract().source_hash != bundle.resources[&address].digest
                || entry.intent.is_some()
                || pending
                    .authorization
                    .base
                    .resource_digests
                    .contains_key(&graph_address(graph))
            {
                return Err(refusal(
                    "deployment_authority_changed",
                    "prepared graph creation differs from original input",
                ));
            }
            continue;
        }
        if let Some(delete) = &entry.delete {
            if bundle.resources.contains_key(&graph_address(graph))
                || pending.authorization.base.schema_contracts.get(graph) != Some(&delete.contract)
                || delete.root
                    != format!(
                        "{}/graphs/{graph}.omni",
                        bundle.canonical_root.trim_start_matches("file://")
                    )
                || entry.create.is_some()
                || entry.intent.is_some()
            {
                return Err(refusal(
                    "deployment_authority_changed",
                    "graph deletion differs from original input and managed identity",
                ));
            }
            continue;
        }
        if !bundle.resources.contains_key(&graph_address(graph)) {
            return Err(refusal(
                "deployment_authority_changed",
                "removed graph has no accepted deletion authority",
            ));
        }
        let schema_changed =
            state.applied_revision.resources[&address].digest != bundle.resources[&address].digest;
        if schema_changed != entry.intent.is_some()
            || entry.intent.as_ref().is_some_and(|intent| {
                intent.actor() != pending.authorization.authority.actor.as_deref()
                    || intent.base_contract() != &pending.authorization.base.schema_contracts[graph]
                    || intent.desired_contract().source_hash != bundle.resources[&address].digest
            })
        {
            return Err(refusal(
                "deployment_authority_changed",
                "prepared schema is not bound to original input and actor",
            ));
        }
    }
    Ok(())
}

decide_seam! { pub static DEPLOYMENT_BEFORE_ACCEPTANCE = ("deployment.before_acceptance", Unreachable, [Fail]); }
decide_seam! { pub static DEPLOYMENT_AFTER_RESULT = ("deployment.after_result", Unreachable, [Fail]); }
decide_seam! { pub static DEPLOYMENT_AFTER_ACCEPTANCE = ("deployment.after_acceptance", Unreachable, [Fail]); }
decide_seam! { pub static DEPLOYMENT_AFTER_STARTED = ("deployment.after_started", Unreachable, [Fail]); }
decide_seam! { pub static DEPLOYMENT_AFTER_SCHEMA = ("deployment.after_schema", Unreachable, [Fail]); }
decide_seam! { pub static DEPLOYMENT_AFTER_SETTLEMENT_INTENT = ("deployment.after_settlement_intent", Unreachable, [Fail]); }
decide_seam! { pub static DEPLOYMENT_BEFORE_RESULT = ("deployment.before_result", Unreachable, [Fail]); }

async fn finish(
    store: &ClusterStore,
    state: &mut ClusterState,
    cas: &str,
    bundle: &DeploymentBundle,
) -> Result<DeploymentResult, Diagnostic> {
    let pending = state.outstanding.as_ref().unwrap().clone();
    if pending.graphs.values().any(|entry| !matches!(&entry.state, GraphDeploymentState::Settled { result } if result.terminal())) {
        return Err(refusal("deployment_outcome_unknown", "deployment has unresolved graph effects"));
    }
    if pending.graphs.values().any(|entry| entry.delete.is_some() && !matches!(&entry.state, GraphDeploymentState::Settled { result } if matches!(result.as_ref(), GraphDeploymentResult::Deleted { .. }))) {
        return Err(refusal("deployment_outcome_unknown", "accepted graph deletions must complete before deployment settlement"));
    }
    let before_resources = serde_json::to_value(&state.applied_revision.resources).unwrap();
    let mut result = result_from_pending(state, &pending);
    for (graph, outcome) in &result.graphs {
        if !outcome.accepted() {
            continue;
        }
        if let GraphDeploymentResult::Schema {
            result:
                SchemaApplySettlement::Committed { contract, .. }
                | SchemaApplySettlement::NoOp { contract, .. },
        }
        | GraphDeploymentResult::Created { contract, .. } = outcome
        {
            state
                .applied_revision
                .schema_contracts
                .as_mut()
                .unwrap()
                .insert(graph.clone(), contract.clone());
        }
        if matches!(outcome, GraphDeploymentResult::Deleted { .. }) {
            state
                .applied_revision
                .schema_contracts
                .as_mut()
                .unwrap()
                .remove(graph);
        }
        let belongs = |address: &str| match resource_kind(address) {
            ResourceKind::Graph(id) | ResourceKind::Schema(id) => id == *graph,
            ResourceKind::Query { graph: id, .. } => id == *graph,
            _ => false,
        };
        state
            .applied_revision
            .resources
            .retain(|address, _| !belongs(address));
        state
            .resource_statuses
            .retain(|address, _| !belongs(address));
        for (address, resource) in &bundle.resources {
            if belongs(address) {
                state
                    .applied_revision
                    .resources
                    .insert(address.clone(), resource.clone());
                set_resource_status_applied(state, address);
            }
        }
    }
    // Runtime configuration has no engine publication. Once every graph effect
    // is terminal, publish its desired projection in this same ledger CAS.
    // A failed schema retains its old schema/query catalog; runtime bindings
    // still converge for that surviving graph. Never bind a not-created graph.
    let achieved_graphs = graph_ids(state);
    for graph in &achieved_graphs {
        if let Some(desired) = bundle.resources.get(&graph_address(graph)) {
            let resource = state
                .applied_revision
                .resources
                .get_mut(&graph_address(graph))
                .unwrap();
            resource.embedding_provider = desired.embedding_provider.clone();
            resource.external_blob_policy = desired.external_blob_policy.clone();
        }
    }
    state.applied_revision.resources.retain(|address, _| {
        !matches!(
            resource_kind(address),
            ResourceKind::Policy(_) | ResourceKind::EmbeddingProvider(_)
        )
    });
    state.resource_statuses.retain(|address, _| {
        !matches!(
            resource_kind(address),
            ResourceKind::Policy(_) | ResourceKind::EmbeddingProvider(_)
        )
    });
    for (address, resource) in &bundle.resources {
        match resource_kind(address) {
            ResourceKind::EmbeddingProvider(_) => {
                state
                    .applied_revision
                    .resources
                    .insert(address.clone(), resource.clone());
                set_resource_status_applied(state, address);
            }
            ResourceKind::Policy(_) => {
                let mut resource = resource.clone();
                if let Some(bindings) = resource.applies_to.as_mut() {
                    bindings.retain(|binding| {
                        binding == "cluster"
                            || binding
                                .strip_prefix("graph.")
                                .is_some_and(|graph| achieved_graphs.iter().any(|id| id == graph))
                    });
                    if bindings.is_empty() {
                        continue;
                    }
                }
                state
                    .applied_revision
                    .resources
                    .insert(address.clone(), resource);
                set_resource_status_applied(state, address);
            }
            _ => {}
        }
    }
    for graph in &achieved_graphs {
        let address = graph_address(graph);
        let digest = expected_state_graph_resource_digest(
            state,
            graph,
            &state.applied_revision.resources[&address],
        );
        state
            .applied_revision
            .resources
            .get_mut(&address)
            .unwrap()
            .digest = digest;
    }
    let achieved_resources = serde_json::to_value(&state.applied_revision.resources).unwrap();
    result.converged = result.graphs.values().all(GraphDeploymentResult::accepted)
        && achieved_resources == serde_json::to_value(&bundle.resources).unwrap();
    if achieved_resources != before_resources
        || state.applied_revision.schema_contracts.as_ref()
            != Some(&pending.authorization.base.schema_contracts)
        || (result.converged
            && state.applied_revision.config_digest.as_deref() != Some(&bundle.config_digest))
    {
        state.applied_revision.result_revision = Some(
            state
                .applied_revision
                .result_revision
                .unwrap()
                .checked_add(1)
                .ok_or_else(|| refusal("revision_exhausted", "achieved revision exhausted"))?,
        );
    }
    state.applied_revision.config_digest = result.converged.then(|| bundle.config_digest.clone());
    result.result_revision = state.applied_revision.result_revision.unwrap();
    result.config_digest = state.applied_revision.config_digest.clone();
    if encoded_size(&result)? > pending.reserved_result_bytes {
        return Err(refusal(
            "deployment_reservation_exceeded",
            "terminal result exceeds its acceptance reservation",
        ));
    }
    state
        .deployment_results
        .as_mut()
        .unwrap()
        .push(result.clone());
    state.outstanding = None;
    if encoded_size(state)?.saturating_add(20) > pending.reserved_ledger_bytes {
        return Err(refusal(
            "deployment_reservation_exceeded",
            "terminal ledger exceeds its acceptance reservation",
        ));
    }
    seams::fail(&DEPLOYMENT_BEFORE_RESULT)?;
    replace(store, state, cas).await?;
    seams::fail(&DEPLOYMENT_AFTER_RESULT)?;
    Ok(result)
}

/// Exact achieved contracts for a runtime activation candidate. The caller
/// must bind all installed graph handles to this map before activation.
pub async fn applied_deployment_contracts(
    admission: &ClusterAdmission,
    result_revision: u64,
) -> Result<BTreeMap<String, omnigraph::db::SchemaContractDigest>, Diagnostic> {
    admission.validate_deployment().await?;
    let store = ClusterStore::for_storage_root(admission.canonical_root())?;
    let (state, _) = read_existing(&store).await?;
    require_v2(&state)?;
    if state.outstanding.is_some()
        || state.applied_revision.result_revision != Some(result_revision)
    {
        return Err(refusal(
            "activation_stale",
            "achieved revision changed while capturing activation contracts",
        ));
    }
    Ok(state.applied_revision.schema_contracts.unwrap())
}

/// Reconcile only the original accepted invocation. Never resumes its schema
/// execution and never reads local desired configuration.
pub async fn reconcile_deployment(
    root: &str,
    id: &str,
    writers_stopped: bool,
    caller: &DeploymentCaller,
) -> Result<DeploymentLookup, Diagnostic> {
    let store = ClusterStore::for_storage_root(root)?;
    let (before, _) = read_existing(&store).await?;
    require_v2(&before)?;
    lookup_policies(&store, &before, caller, Some(id)).await?;
    let existing = lookup(&before, id)?;
    if !matches!(existing, DeploymentLookup::Outstanding { .. }) {
        return Ok(existing);
    }
    if !writers_stopped {
        return Err(refusal(
            "writers_stopped_required",
            "recovery requires --writers-stopped and settled prior graph/control I/O",
        ));
    }
    let admission = admission::acquire_with_store(
        &store,
        ClusterAdmissionPurpose::Reconcile {
            deployment_id: id.to_owned(),
        },
    )
    .await?
    .ok_or_else(|| refusal("ledger_upgrade_required", "recovery requires ledger v2"))?;
    let (mut state, mut cas) = read_existing(&store).await?;
    let policy = lookup_policies(&store, &state, caller, Some(id)).await?;
    let pending = state
        .outstanding
        .as_ref()
        .ok_or_else(|| refusal("deployment_changed", "original outstanding record changed"))?
        .clone();
    if pending.id != id
        || pending.authorization.base.result_revision
            != state.applied_revision.result_revision.unwrap()
        || pending.authorization.base.resource_digests != state_resource_digests(&state)
        || Some(&pending.authorization.base.schema_contracts)
            != state.applied_revision.schema_contracts.as_ref()
        || pending.authorization.policy_digests != policy.digests
    {
        return Err(refusal(
            "deployment_base_changed",
            "outstanding achieved authority changed",
        ));
    }
    let bundle = store.read_deployment_bundle(&pending.input_digest).await?;
    validate_bundle(&bundle, admission.canonical_root())?;
    validate_pending_input(&state, &pending, &bundle)?;
    install_catalog_payloads(&store, &state, &bundle).await?;
    for (graph, entry) in pending.graphs {
        let result = if let Some(delete) = &entry.delete {
            authorize_graph_deletion(&policy, caller, &graph)?;
            if matches!(entry.state, GraphDeploymentState::Settled { .. }) {
                continue;
            }
            if store.canonical_managed_graph_root(&graph)? != delete.root {
                return Err(refusal(
                    "cluster_graph_root_mismatch",
                    "graph deletion root differs from original authority",
                ));
            }
            // Before Started, a readable complete managed graph is still required.
            // Afterwards a partial root is expected and only durable root authority
            // can authorize completion; never recreate it to perform this check.
            if store
                .graph_root_exists(&delete.root)
                .await
                .map_err(|error| refusal("deployment_outcome_unknown", error.to_string()))?
            {
                let opened = if matches!(entry.state, GraphDeploymentState::NotStarted) {
                    open_graph(&store, &graph, &policy, true).await
                } else {
                    // A partial purge need not have a complete recovery catalog.
                    // Still reject a readable foreign manifest before deleting it.
                    Omnigraph::open_read_only(&delete.root)
                        .await
                        .map_err(|error| refusal("graph_unavailable", error.to_string()))
                };
                match opened {
                    Ok(db) => {
                        let observed = db.schema_contract_digest();
                        let matches_authority =
                            if matches!(entry.state, GraphDeploymentState::NotStarted) {
                                observed == delete.contract
                            } else {
                                // A partial purge may remove newer manifests before old
                                // ones. An older schema of this lifetime is still owned
                                // by the accepted deletion; a replacement lifetime is not.
                                observed.schema_identity_domain
                                    == delete.contract.schema_identity_domain
                            };
                        if !matches_authority {
                            return Err(refusal(
                                "deployment_outcome_unknown",
                                "graph deletion root contains a foreign schema identity",
                            ));
                        }
                    }
                    Err(error) if matches!(entry.state, GraphDeploymentState::NotStarted) => {
                        return Err(error);
                    }
                    _ => {}
                }
            }
            let current = state
                .outstanding
                .as_mut()
                .unwrap()
                .graphs
                .get_mut(&graph)
                .unwrap();
            current.state = GraphDeploymentState::Started;
            current.recovery_executor = Some(caller.authority()?);
            cas = replace(&store, &mut state, &cas).await?;
            seams::fail(&DEPLOYMENT_AFTER_STARTED)?;
            complete_graph_deletion(&store, &graph, delete, id, &admission).await?;
            seams::fail(&DEPLOYMENT_AFTER_SCHEMA)?;
            GraphDeploymentResult::Deleted {
                contract: delete.contract.clone(),
            }
        } else {
            match entry.state {
                GraphDeploymentState::Settled { .. } => continue,
                GraphDeploymentState::NotStarted
                    if entry.create.is_none() && entry.intent.is_none() =>
                {
                    admission
                        .validate_graph_uri(&store.graph_root(&graph))
                        .await?;
                    let db = open_graph(&store, &graph, &policy, true).await?;
                    let snapshot = db
                        .snapshot_of(ReadTarget::branch("main"))
                        .await
                        .map_err(|error| refusal("graph_unavailable", error.to_string()))?;
                    let observed = db.schema_contract_digest();
                    if pending.authorization.base.schema_contracts.get(&graph) != Some(&observed) {
                        return Err(refusal(
                            "deployment_outcome_unknown",
                            "graph identity changed before original catalog settlement",
                        ));
                    }
                    GraphDeploymentResult::QueryOnly {
                        graph_manifest_version: snapshot.graph_manifest_version(),
                        schema_digest: bundle.resources[&schema_address(&graph)].digest.clone(),
                    }
                }
                GraphDeploymentState::NotStarted => GraphDeploymentResult::NotAttempted,
                GraphDeploymentState::Started if entry.create.is_some() => {
                    state
                        .outstanding
                        .as_mut()
                        .unwrap()
                        .graphs
                        .get_mut(&graph)
                        .unwrap()
                        .recovery_executor = Some(caller.authority()?);
                    cas = replace(&store, &mut state, &cas).await?;
                    match Omnigraph::settle_prepared_graph_create_after_quiescence(
                        entry.create.as_ref().unwrap(),
                    )
                    .await
                    .map_err(|error| refusal("deployment_outcome_unknown", error.to_string()))?
                    {
                        GraphCreateReconciliation::Created {
                            graph_manifest_version,
                            contract,
                        } => GraphDeploymentResult::Created {
                            graph_manifest_version,
                            contract,
                        },
                        GraphCreateReconciliation::Absent => GraphDeploymentResult::Refused {
                            code: "graph_not_created".into(),
                        },
                        GraphCreateReconciliation::Unknown => {
                            return Err(refusal(
                                "deployment_outcome_unknown",
                                "original graph creation cannot be proved from its exact genesis identity",
                            ));
                        }
                    }
                }
                GraphDeploymentState::Started => {
                    let original = entry.intent.as_ref().ok_or_else(|| {
                        refusal(
                            "invalid_state",
                            "query-only work cannot carry started graph effects",
                        )
                    })?;
                    if let DeploymentCaller::AuthenticatedIdentity(identity) = caller {
                        policy.check_graph(identity.actor(), &graph, PolicyAction::Read)?;
                        policy.check_graph(identity.actor(), &graph, PolicyAction::SchemaApply)?;
                    }
                    admission
                        .validate_graph_uri(&store.graph_root(&graph))
                        .await?;
                    let db = open_graph(&store, &graph, &policy, false).await?;
                    // Persist one settlement identity before its first invocation.
                    // Later operators adopt it, preserving its authored receipt.
                    let settlement = match entry.settlement {
                    Some(settlement) => settlement,
                    None => db
                        .prepare_schema_settlement_as(original, caller.actor())
                        .await
                        .map_err(|error| {
                            refusal(
                                "deployment_outcome_unknown",
                                format!("could not prepare original settlement evidence under admission {}: {error}", admission.lock_id()),
                            )
                        })?,
                };
                    let current = state
                        .outstanding
                        .as_mut()
                        .unwrap()
                        .graphs
                        .get_mut(&graph)
                        .unwrap();
                    current.settlement = Some(settlement.clone());
                    current.recovery_executor = Some(caller.authority()?);
                    cas = replace(&store, &mut state, &cas).await?;
                    seams::fail(&DEPLOYMENT_AFTER_SETTLEMENT_INTENT)?;
                    let result = db
                    .settle_prepared_schema_as(original, &settlement, caller.actor())
                    .await
                    .map_err(|error| {
                        refusal(
                            "deployment_outcome_unknown",
                            format!("original deployment remains unresolved under admission {}: {error}", admission.lock_id()),
                        )
                    })?;
                    if matches!(result, SchemaApplySettlement::Unknown) {
                        return Err(refusal(
                            "deployment_outcome_unknown",
                            "protected publication evidence is unavailable",
                        ));
                    }
                    GraphDeploymentResult::Schema { result }
                }
            }
        };
        state
            .outstanding
            .as_mut()
            .unwrap()
            .graphs
            .get_mut(&graph)
            .unwrap()
            .state = GraphDeploymentState::Settled {
            result: Box::new(result),
        };
        cas = replace(&store, &mut state, &cas).await?;
    }
    let result = finish(&store, &mut state, &cas, &bundle).await?;
    // Exact publication is not a proof that all accepted native/control I/O
    // has stopped. Retain admission until explicit operator quiescence/unlock.
    drop(admission);
    Ok(DeploymentLookup::Complete { result })
}
