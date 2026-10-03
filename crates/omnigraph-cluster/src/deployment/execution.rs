use super::*;
use crate::authorization::AppliedPolicies;
use omnigraph_policy::PolicyAction;

async fn read_existing(store: &ClusterStore) -> Result<(ClusterState, String), Diagnostic> {
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
    manage: bool,
) -> Result<AppliedPolicies, Diagnostic> {
    caller.authority()?;
    let policies = match caller {
        DeploymentCaller::StorageOwner { .. } => {
            AppliedPolicies::load_optional(store, state).await?
        }
        DeploymentCaller::AuthenticatedIdentity(identity) => {
            let policies = AppliedPolicies::load(store, state).await?;
            if manage {
                policies.check_cluster(identity.actor())?;
            }
            for address in state.applied_revision.resources.keys() {
                if let ResourceKind::Graph(graph) = resource_kind(address) {
                    policies.check_graph(identity.actor(), &graph, PolicyAction::Read)?;
                }
            }
            policies
        }
    };
    Ok(policies)
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
    policies(&store, &state, caller, true).await?;
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

fn lookup(state: &ClusterState, requested: &str) -> Result<DeploymentLookup, Diagnostic> {
    let id = DeploymentId::parse(requested)?;
    if Some(id.ledger.as_str()) != state.ledger_id.as_deref() {
        return Ok(DeploymentLookup::DifferentLedger);
    }
    if let Some(pending) = &state.outstanding {
        if pending.id == requested {
            return Ok(DeploymentLookup::Outstanding {
                id: requested.to_owned(),
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
            acceptance: "unknown",
            outcome: "unknown",
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
    let (before, _) = read_existing(&store).await?;
    if before.version == 2 {
        return deployment_status(root, None, caller).await;
    }
    // Refuse unauthorized conversion before acquiring durable admission; a
    // denied caller must not strand a lock. Recheck current policy under it.
    policies(&store, &before, caller, true).await?;
    let mut observations = store.observations();
    let mut guard = store
        .acquire_lock("upgrade_ledger", &mut observations)
        .await?;
    // Lost conversion acknowledgement must not silently release v2 admission.
    guard.hold_on_drop();
    let (mut state, cas) = read_existing(&store).await?;
    if state.version == 2 {
        return Err(refusal(
            "ledger_changed",
            "ledger changed during conversion; inspect original root",
        ));
    }
    policies(&store, &state, caller, true).await?;
    authorization::refuse_pending_recovery(&store).await?;
    serve::read_snapshot_with_store(&store)
        .await
        .map_err(|mut diagnostics| diagnostics.remove(0))?;
    state.applied_revision.schema_contracts =
        Some(capture_applied_graph_contracts(&store, &state).await?);
    state.version = 2;
    state.mode = Some(DeploymentMode::Offline);
    state.ledger_id = Some(Ulid::new().to_string());
    state.next_sequence = Some(1);
    state.applied_revision.result_revision = Some(0);
    state.deployment_results = Some(Vec::new());
    replace(&store, &mut state, &cas).await?;
    store
        .force_unlock(guard.lock_id(), &mut observations)
        .await?;
    deployment_status(root, None, caller).await
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

fn validate_bundle(bundle: &DeploymentBundle, root: &str) -> Result<(), Diagnostic> {
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
    Ok(())
}

fn validate_scope(
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
        match resource_kind(address) {
            ResourceKind::Schema(_) if old.is_some() && new.is_some() => {}
            ResourceKind::Query { graph, .. } if before.contains_key(&graph_address(&graph)) => {}
            ResourceKind::Graph(_)
                if old.zip(new).is_some_and(|(old, new)| {
                    old.embedding_provider == new.embedding_provider
                        && old.external_blob_policy == new.external_blob_policy
                        && old.applies_to == new.applies_to
                        && old.embedding_profile == new.embedding_profile
                }) => {}
            _ => {
                return Err(refusal(
                    "offline_deployment_scope",
                    format!(
                        "{address} changes fixed inventory or bindings; offline deployment supports existing schema/query changes only"
                    ),
                ));
            }
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
) -> Result<Omnigraph, Diagnostic> {
    let uri = store.graph_root(graph);
    Omnigraph::ensure_no_pending_recovery(&uri)
        .await
        .map_err(|error| refusal("graph_recovery_required", error.to_string()))?;
    let db = Omnigraph::open(&uri)
        .await
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
        restart_required: true,
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
        .iter()
        .map(|(graph, count)| (*count).max(*new_counts.get(graph).unwrap_or(&0)))
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

/// Submit a frozen offline deployment. `report_id` runs before acceptance and
/// graph effects, so a severed caller can later look up the original identity.
/// Resubmission with an existing ID only observes; it never executes again.
pub async fn apply_deployment(
    config_dir: impl AsRef<Path>,
    requested_id: Option<&str>,
    caller: &DeploymentCaller,
    report_id: impl FnOnce(&str, &str, &str),
) -> Result<DeploymentLookup, Diagnostic> {
    let captured = config::capture_desired(config_dir.as_ref());
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
    let store = store_for(&desired.config_dir, desired.storage_root.as_deref())?;
    let root = store.canonical_root()?;
    let bundle = bundle_from_capture(&desired, captured.sources, root.clone());
    validate_bundle(&bundle, &root)?;
    let input_digest = sha256_hex(
        &serde_json::to_vec(&bundle)
            .map_err(|error| refusal("deployment_encode", error.to_string()))?,
    );
    // Existing identities are lookup-only, including while their owner holds
    // the lock. Input equality is required before exposing the original result.
    let (before, _) = read_existing(&store).await?;
    require_v2(&before)?;
    if !desired.state_lock {
        return Err(refusal(
            "deployment_requires_lock",
            "offline deployment requires cluster admission",
        ));
    }
    policies(&store, &before, caller, true).await?;
    if let Some(id) = requested_id {
        let existing = lookup(&before, id)?;
        if !matches!(existing, DeploymentLookup::NotRecorded) {
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
    let admission = admission::acquire_with_store(&store, ClusterAdmissionPurpose::Deployment)
        .await?
        .ok_or_else(|| {
            refusal(
                "ledger_upgrade_required",
                "offline deployment requires ledger v2",
            )
        })?;
    let (mut state, mut cas) = read_existing(&store).await?;
    require_v2(&state)?;
    let policy = policies(&store, &state, caller, true).await?;
    authorization::refuse_pending_recovery(&store).await?;
    let effects = validate_scope(&state, &bundle)?;
    let sequence = state.next_sequence.unwrap();
    let next = sequence
        .checked_add(1)
        .ok_or_else(|| refusal("sequence_exhausted", "deployment sequence exhausted"))?;
    let id = requested_id.map(str::to_owned).unwrap_or_else(|| {
        format!(
            "{}:{sequence}:{}",
            state.ledger_id.as_ref().unwrap(),
            Ulid::new()
        )
    });
    let parsed_id = DeploymentId::parse(&id)?;
    if Some(parsed_id.ledger.as_str()) != state.ledger_id.as_deref()
        || parsed_id.sequence != sequence
    {
        return Err(refusal(
            "deployment_id_stale",
            "new deployment must use this ledger's exact next sequence",
        ));
    }
    report_id(&id, admission.canonical_root(), admission.lock_id());
    let mut graphs = BTreeMap::new();
    let mut handles = BTreeMap::new();
    for graph in graph_ids(&state) {
        admission
            .validate_graph_uri(&store.graph_root(&graph))
            .await?;
        let schema_address = schema_address(&graph);
        let schema_changed = state.applied_revision.resources[&schema_address].digest
            != bundle.resources[&schema_address].digest;
        let affected = effects
            .iter()
            .any(|effect| match resource_kind(&effect.resource) {
                ResourceKind::Graph(id) | ResourceKind::Schema(id) => id == graph,
                ResourceKind::Query { graph: id, .. } => id == graph,
                _ => false,
            });
        let db = open_graph(&store, &graph, &policy).await?;
        let snapshot = db
            .snapshot_of(ReadTarget::branch("main"))
            .await
            .map_err(|error| refusal("graph_unavailable", error.to_string()))?;
        if !schema_changed
            && state
                .applied_revision
                .schema_contracts
                .as_ref()
                .unwrap()
                .get(&graph)
                != Some(&db.schema_contract_digest())
        {
            return Err(refusal(
                "applied_schema_drift",
                format!("graph {graph} requires an explicit schema correction"),
            ));
        }
        if !affected {
            continue;
        }
        let intent = if schema_changed {
            if let DeploymentCaller::AuthenticatedIdentity(identity) = caller {
                policy.check_graph(identity.actor(), &graph, PolicyAction::SchemaApply)?;
            }
            Some(
                db.prepare_schema_apply_as(source_for(&bundle, &schema_address)?, caller.actor())
                    .await
                    .map_err(|error| refusal("schema_preflight_failed", error.to_string()))?,
            )
        } else {
            None
        };
        graphs.insert(
            graph.clone(),
            GraphDeployment {
                intent,
                observed_manifest_version: snapshot.graph_manifest_version(),
                state: GraphDeploymentState::NotStarted,
                settlement: None,
                recovery_executor: None,
            },
        );
        handles.insert(graph, db);
    }
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
        input_digest: input_digest.clone(),
        policy_digests: policy.digests.clone(),
        effects,
    };
    state.next_sequence = Some(next);
    state.outstanding = Some(OutstandingDeployment {
        id: id.clone(),
        input_digest: input_digest.clone(),
        authorization,
        graphs,
        reserved_ledger_bytes: 0,
        reserved_result_bytes: 0,
    });
    reserve_completion(&mut state, &bundle)?;
    if store.write_deployment_bundle(&bundle).await? != input_digest {
        return Err(refusal(
            "deployment_input_invalid",
            "bundle encoding identity changed",
        ));
    }
    // Catalog query payloads are immutable and unreachable until the final
    // achieved-projection CAS. Execution never reopens source files.
    for (address, resource) in &bundle.resources {
        let kind = resource_kind(address);
        if matches!(kind, ResourceKind::Query { .. }) {
            store
                .write_payload(&kind, &resource.digest, source_for(&bundle, address)?)
                .await
                .map_err(|error| refusal("resource_payload_write_error", error))?;
        }
    }
    seams::fail(&DEPLOYMENT_BEFORE_ACCEPTANCE)?;
    cas = replace(&store, &mut state, &cas).await?;
    seams::fail(&DEPLOYMENT_AFTER_ACCEPTANCE)?;
    for (graph, db) in handles {
        let entry = state.outstanding.as_ref().unwrap().graphs[&graph].clone();
        let result = if let Some(intent) = &entry.intent {
            state
                .outstanding
                .as_mut()
                .unwrap()
                .graphs
                .get_mut(&graph)
                .unwrap()
                .state = GraphDeploymentState::Started;
            cas = replace(&store, &mut state, &cas).await?;
            seams::fail(&DEPLOYMENT_AFTER_STARTED)?;
            let applied = db.apply_prepared_schema_as(intent, caller.actor()).await
                .map_err(|_| refusal("deployment_outcome_unknown", format!("deployment {id} remains outstanding; establish quiescence and reconcile its original identity")))?;
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
        cas = replace(&store, &mut state, &cas).await?;
    }
    let result = finish(&store, &mut state, &cas, &bundle).await?;
    // Exact publication is not a proof that all accepted native/control I/O
    // has stopped. Retain admission until explicit operator quiescence/unlock.
    drop(admission);
    Ok(DeploymentLookup::Complete { result })
}

fn validate_pending_input(
    state: &ClusterState,
    pending: &OutstandingDeployment,
    bundle: &DeploymentBundle,
) -> Result<(), Diagnostic> {
    let effects = validate_scope(state, bundle)?;
    let affected: BTreeSet<_> = effects
        .iter()
        .filter_map(|effect| match resource_kind(&effect.resource) {
            ResourceKind::Graph(graph)
            | ResourceKind::Schema(graph)
            | ResourceKind::Query { graph, .. } => Some(graph),
            _ => None,
        })
        .collect();
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
        let schema_changed =
            state.applied_revision.resources[&address].digest != bundle.resources[&address].digest;
        if schema_changed != entry.intent.is_some()
            || entry.intent.as_ref().is_some_and(|intent| {
                intent.actor() != pending.authorization.authority.actor.as_deref()
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
    let mut result = result_from_pending(state, &pending);
    for (graph, outcome) in &result.graphs {
        if !outcome.accepted() {
            continue;
        }
        if let GraphDeploymentResult::Schema {
            result:
                SchemaApplySettlement::Committed { contract, .. }
                | SchemaApplySettlement::NoOp { contract, .. },
        } = outcome
        {
            state
                .applied_revision
                .schema_contracts
                .as_mut()
                .unwrap()
                .insert(graph.clone(), contract.clone());
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
    let digests = state_resource_digests(state);
    result.converged = digests
        == bundle
            .resources
            .iter()
            .map(|(address, resource)| (address.clone(), resource.digest.clone()))
            .collect();
    if digests != pending.authorization.base.resource_digests
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
    policies(&store, &before, caller, true).await?;
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
    let policy = policies(&store, &state, caller, true).await?;
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
    for (graph, entry) in pending.graphs {
        let result = match entry.state {
            GraphDeploymentState::Settled { .. } => continue,
            GraphDeploymentState::NotStarted => GraphDeploymentResult::NotAttempted,
            GraphDeploymentState::Started => {
                let original = entry.intent.as_ref().ok_or_else(|| {
                    refusal(
                        "invalid_state",
                        "query-only work cannot carry started graph effects",
                    )
                })?;
                if let DeploymentCaller::AuthenticatedIdentity(identity) = caller {
                    policy.check_graph(identity.actor(), &graph, PolicyAction::SchemaApply)?;
                }
                admission
                    .validate_graph_uri(&store.graph_root(&graph))
                    .await?;
                let db = open_graph(&store, &graph, &policy).await?;
                // Persist one settlement identity before its first invocation.
                // Later operators adopt it, preserving its authored receipt.
                let settlement = match entry.settlement {
                    Some(settlement) => settlement,
                    None => db
                        .prepare_schema_settlement_as(original, caller.actor())
                        .await
                        .map_err(|_| {
                            refusal(
                                "deployment_outcome_unknown",
                                "could not prepare original settlement evidence",
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
                    .map_err(|_| {
                        refusal(
                            "deployment_outcome_unknown",
                            "original deployment remains unresolved",
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
