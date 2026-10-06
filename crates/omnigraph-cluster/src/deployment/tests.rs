use super::*;
use std::sync::Arc;

fn owner() -> DeploymentCaller {
    DeploymentCaller::storage_owner(Some("principal:owner".into()))
}

async fn unlock(root: &str) {
    if let Some(lock) = deployment_status(root, None, &owner())
        .await
        .unwrap()
        .lock_id
    {
        force_unlock_storage_root(root, &lock).await.unwrap();
    }
}

async fn bootstrap(dir: &Path) -> DeploymentResult {
    let result = apply_deployment(dir, None, &owner(), &BTreeMap::new(), |_, _, _| {})
        .await
        .unwrap();
    let DeploymentLookup::Complete { result } = result else {
        panic!("{result:?}");
    };
    assert!(result.converged);
    result
}

fn add_second_graph(dir: &Path) {
    let path = dir.join(CLUSTER_CONFIG_FILE);
    let source = fs::read_to_string(&path).unwrap();
    fs::write(
        path,
        source
            .replace("graphs:\n", "graphs:\n  second:\n    schema: ./people.pg\n")
            .replace("applies_to: [knowledge]", "applies_to: [knowledge, second]"),
    )
    .unwrap();
}

#[tokio::test]
async fn bootstrap_and_add_graph_use_one_v2_protocol_without_reset() {
    let dir = crate::tests::identity_fixture();
    let root = dir.path().to_str().unwrap();
    let first = bootstrap(dir.path()).await;
    assert!(matches!(
        first.graphs["knowledge"],
        GraphDeploymentResult::Created { .. }
    ));
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap()).unwrap();
    assert_eq!(state["version"], 2);
    assert!(state.get("mode").is_none());
    let uri = dir.path().join("graphs/knowledge.omni");
    let original = Omnigraph::open_read_only(uri.to_str().unwrap())
        .await
        .unwrap();
    let before = original.schema_contract_digest();
    let version = original
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .graph_manifest_version();
    unlock(root).await;
    add_second_graph(dir.path());
    let added = bootstrap(dir.path()).await;
    assert_eq!(added.graphs.len(), 1);
    assert!(matches!(
        added.graphs["second"],
        GraphDeploymentResult::Created { .. }
    ));
    let second = Omnigraph::open_read_only(dir.path().join("graphs/second.omni").to_str().unwrap())
        .await
        .unwrap();
    assert_ne!(
        second.schema_contract_digest().schema_identity_domain,
        before.schema_identity_domain
    );
    assert_eq!(original.schema_contract_digest(), before);
    assert_eq!(
        original
            .snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .graph_manifest_version(),
        version
    );
    let snapshot = read_serving_snapshot_from_storage(root).await.unwrap();
    assert_eq!(snapshot.graphs.len(), 2);
    assert!(
        snapshot
            .policies
            .iter()
            .any(|policy| policy.applies_to == ["graph.knowledge", "graph.second"])
    );
    assert!(
        snapshot.diagnostics.is_empty(),
        "{:?}",
        snapshot.diagnostics
    );
    unlock(root).await;
    let admission = acquire_cluster_admission(root, ClusterAdmissionPurpose::Serve)
        .await
        .unwrap()
        .unwrap();
    let captured = capture_deployment(dir.path(), &BTreeMap::new()).unwrap();
    let before = fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap();
    let preview = preview_deployment_serving_snapshot(&captured, &admission, &owner())
        .await
        .unwrap();
    assert_eq!(
        preview
            .graphs
            .iter()
            .map(|graph| &graph.graph_id)
            .collect::<Vec<_>>(),
        snapshot
            .graphs
            .iter()
            .map(|graph| &graph.graph_id)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        preview
            .policies
            .iter()
            .map(|policy| (&policy.source, &policy.applies_to))
            .collect::<Vec<_>>(),
        snapshot
            .policies
            .iter()
            .map(|policy| (&policy.source, &policy.applies_to))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        preview
            .queries
            .iter()
            .map(|query| (&query.graph_id, &query.name, &query.source))
            .collect::<Vec<_>>(),
        snapshot
            .queries
            .iter()
            .map(|query| (&query.graph_id, &query.name, &query.source))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        before
    );
}

#[tokio::test]
async fn held_server_owner_applies_twice_and_records_exact_activation() {
    let dir = crate::tests::identity_fixture();
    let root = dir.path().to_str().unwrap();
    bootstrap(dir.path()).await;
    unlock(root).await;
    let admission = acquire_cluster_admission(root, ClusterAdmissionPurpose::Serve)
        .await
        .unwrap()
        .unwrap();
    let db = Arc::new(
        Omnigraph::open(dir.path().join("graphs/knowledge.omni").to_str().unwrap())
            .await
            .unwrap(),
    );
    let handles = BTreeMap::from([("knowledge".into(), db.clone())]);
    let base = db.schema_contract_digest();
    for field in ["email", "phone"] {
        fs::write(
            dir.path().join("people.pg"),
            crate::tests::SCHEMA.replace("age: I32?", &format!("age: I32?\n  {field}: String?")),
        )
        .unwrap();
        let bundle = capture_deployment(dir.path(), &BTreeMap::new()).unwrap();
        let affected = deployment_affected_graphs(&bundle, &admission, &owner())
            .await
            .unwrap();
        assert_eq!(affected, ["knowledge"]);
        let mut started = false;
        let applied = apply_captured_deployment(
            &bundle,
            None,
            &owner(),
            &admission,
            &handles,
            |_, _, _| {},
            &mut started,
        )
        .await
        .unwrap();
        assert!(started);
        let DeploymentLookup::Complete { result } = applied else {
            panic!("{applied:?}");
        };
        assert!(result.converged);
        assert_eq!(
            db.schema_contract_digest().source_hash,
            bundle.resources["schema.knowledge"].digest
        );
        assert_eq!(
            db.schema_contract_digest().schema_identity_domain,
            base.schema_identity_domain
        );
        let contracts = applied_deployment_contracts(&admission, result.result_revision)
            .await
            .unwrap();
        let activated = record_deployment_activation(
            root,
            &result.id,
            &admission,
            DeploymentActivation {
                process_incarnation: Ulid::new().to_string(),
                result_revision: result.result_revision,
                config_digest: result.config_digest.unwrap(),
            },
            &contracts,
        )
        .await
        .unwrap();
        assert!(!activated.restart_required);
        let status = deployment_status(root, Some(&activated.id), &owner())
            .await
            .unwrap();
        assert!(
            matches!(status.lookup, Some(DeploymentLookup::Complete { result }) if result.activation.is_some())
        );
    }
}

#[tokio::test]
async fn captured_input_is_revalidated_and_preflight_keeps_original_writer() {
    let dir = crate::tests::identity_fixture();
    let root = dir.path().to_str().unwrap();
    bootstrap(dir.path()).await;
    unlock(root).await;
    let admission = acquire_cluster_admission(root, ClusterAdmissionPurpose::Serve)
        .await
        .unwrap()
        .unwrap();
    let original_bytes = fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap();
    let mut bundle = capture_deployment(dir.path(), &BTreeMap::new()).unwrap();
    let mut raw: RawClusterConfig = serde_json::from_str(&bundle.config_semantics).unwrap();
    raw.state.lock = Some(false);
    bundle.config_semantics = serde_json::to_string(&raw).unwrap();
    bundle.config_digest = desired_config_digest_from_semantics(
        &bundle.config_semantics,
        &bundle
            .resources
            .iter()
            .map(|(key, value)| (key.clone(), value.digest.clone()))
            .collect(),
    );
    let mut effects = false;
    let error = apply_captured_deployment(
        &bundle,
        None,
        &owner(),
        &admission,
        &BTreeMap::new(),
        |_, _, _| {},
        &mut effects,
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "deployment_requires_lock");
    assert!(!effects);
    assert_eq!(
        fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        original_bytes
    );
    assert_eq!(
        deployment_status(root, None, &owner())
            .await
            .unwrap()
            .lock_id
            .as_deref(),
        Some(admission.lock_id())
    );
}

#[tokio::test]
async fn graph_creation_refuses_foreign_root_without_holding_new_admission() {
    let dir = crate::tests::identity_fixture();
    let root = dir.path().to_str().unwrap();
    bootstrap(dir.path()).await;
    unlock(root).await;
    add_second_graph(dir.path());
    let foreign = dir.path().join("graphs/second.omni");
    Omnigraph::init(foreign.to_str().unwrap(), crate::tests::SCHEMA)
        .await
        .unwrap();
    let error = apply_deployment(dir.path(), None, &owner(), &BTreeMap::new(), |_, _, _| {})
        .await
        .unwrap_err();
    assert_eq!(error.code, "graph_create_preflight_failed");
    assert!(
        deployment_status(root, None, &owner())
            .await
            .unwrap()
            .lock_id
            .is_none()
    );
}

fn identity(actor: &str) -> DeploymentCaller {
    DeploymentCaller::AuthenticatedIdentity(IdentityAuthorization::authenticated(actor).unwrap())
}

#[tokio::test]
async fn configuration_metadata_does_not_require_unrelated_graph_read() {
    let dir = crate::tests::identity_fixture();
    let root = dir.path().to_str().unwrap();
    add_second_graph(dir.path());
    let config_path = dir.path().join(CLUSTER_CONFIG_FILE);
    let config = fs::read_to_string(&config_path)
        .unwrap()
        .replace("applies_to: [knowledge, second]", "applies_to: [knowledge]");
    fs::write(config_path, config).unwrap();
    let first = bootstrap(dir.path()).await;
    unlock(root).await;
    let before = fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap();
    let mut versions = BTreeMap::new();
    for graph in ["knowledge", "second"] {
        let db = Omnigraph::open_read_only(
            dir.path()
                .join(format!("graphs/{graph}.omni"))
                .to_str()
                .unwrap(),
        )
        .await
        .unwrap();
        versions.insert(
            graph,
            db.snapshot_of(ReadTarget::branch("main"))
                .await
                .unwrap()
                .graph_manifest_version(),
        );
    }
    // Status and exact receipt lookup disclose control metadata, not rows.
    let status = deployment_status(root, Some(&first.id), &identity("principal:owner"))
        .await
        .unwrap();
    assert!(matches!(
        status.lookup,
        Some(DeploymentLookup::Complete { .. })
    ));
    assert!(status.lock_id.is_none());
    assert_eq!(
        fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        before
    );
    let error = deployment_status(root, None, &identity("principal:reader"))
        .await
        .unwrap_err();
    assert_eq!(error.code, "policy_denied");
    // Removing the inventory-wide Read requirement does not grant data access.
    let store = ClusterStore::for_storage_root(root).unwrap();
    let state: ClusterState = serde_json::from_slice(&before).unwrap();
    let policies = crate::authorization::AppliedPolicies::load(&store, &state)
        .await
        .unwrap();
    let error = policies
        .check_graph(
            "principal:owner",
            "second",
            omnigraph_policy::PolicyAction::Read,
        )
        .unwrap_err();
    assert_eq!(error.code, "graph_policy_required");
    // A schema effect on the unbound sibling still refuses before publication,
    // even though this caller may configure the cluster and change knowledge.
    fs::write(
        dir.path().join("people.pg"),
        crate::tests::SCHEMA.replace("age: I32?", "age: I32?\n  email: String?"),
    )
    .unwrap();
    let error = apply_deployment(
        dir.path(),
        None,
        &identity("principal:owner"),
        &BTreeMap::new(),
        |_, _, _| {},
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "graph_policy_required");
    assert_eq!(
        fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        before
    );
    assert!(
        deployment_status(root, None, &owner())
            .await
            .unwrap()
            .lock_id
            .is_none()
    );
    fs::write(dir.path().join("people.pg"), crate::tests::SCHEMA).unwrap();
    let result = apply_deployment(
        dir.path(),
        None,
        &identity("principal:collaborator"),
        &BTreeMap::new(),
        |_, _, _| {},
    )
    .await
    .unwrap();
    let DeploymentLookup::Complete { result } = result else {
        panic!("{result:?}");
    };
    assert!(result.converged);
    assert!(result.graphs.is_empty());
    let after_noop = fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap();
    let repeated = apply_deployment(
        dir.path(),
        Some(&result.id),
        &identity("principal:collaborator"),
        &BTreeMap::new(),
        |_, _, _| panic!("Recorded invocation must not execute again"),
    )
    .await
    .unwrap();
    assert!(matches!(repeated, DeploymentLookup::Complete { .. }));
    assert_eq!(
        fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        after_noop
    );
    for (graph, version) in versions {
        let db = Omnigraph::open_read_only(
            dir.path()
                .join(format!("graphs/{graph}.omni"))
                .to_str()
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            db.snapshot_of(ReadTarget::branch("main"))
                .await
                .unwrap()
                .graph_manifest_version(),
            version,
        );
    }
}

#[tokio::test]
async fn authenticated_deployment_checks_current_policy_before_every_effect() {
    let dir = crate::tests::identity_fixture();
    let root = dir.path().to_str().unwrap();
    let policy_path = dir.path().join("base.policy.yaml");
    let policy = fs::read_to_string(&policy_path)
        .unwrap()
        .replace("groups:\n", "groups:\n  schema_owners: [principal:owner]\n")
        .replace(
            "actors: {group: owners}, actions: [schema_apply]",
            "actors: {group: schema_owners}, actions: [schema_apply]",
        );
    fs::write(&policy_path, policy).unwrap();
    bootstrap(dir.path()).await;
    unlock(root).await;
    let before = fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap();
    add_second_graph(dir.path());
    fs::write(
        dir.path().join("people.pg"),
        crate::tests::SCHEMA.replace("age: I32?", "age: I32?\n  email: String?"),
    )
    .unwrap();
    let error = apply_deployment(
        dir.path(),
        None,
        &identity("principal:collaborator"),
        &BTreeMap::new(),
        |_, _, _| {},
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "policy_denied");
    assert_eq!(
        fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        before
    );
    assert!(!dir.path().join("graphs/second.omni").exists());
    assert!(
        deployment_status(root, None, &owner())
            .await
            .unwrap()
            .lock_id
            .is_none()
    );
    // Desired policy cannot grant the initiating request privileges it lacked
    // in the achieved policy, including a metadata-only deployment.
    let management = dir.path().join("management.policy.yaml");
    let source = fs::read_to_string(&management).unwrap().replace(
        "principal:collaborator]",
        "principal:collaborator, principal:reader]",
    );
    fs::write(management, source).unwrap();
    let error = apply_deployment(
        dir.path(),
        None,
        &identity("principal:reader"),
        &BTreeMap::new(),
        |_, _, _| {},
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "policy_denied");
    assert_eq!(
        fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        before
    );
}

#[tokio::test]
async fn authenticated_deployment_records_the_authenticated_actor() {
    let dir = crate::tests::identity_fixture();
    let root = dir.path().to_str().unwrap();
    bootstrap(dir.path()).await;
    unlock(root).await;
    fs::write(
        dir.path().join("people.pg"),
        crate::tests::SCHEMA.replace("age: I32?", "age: I32?\n  email: String?"),
    )
    .unwrap();
    let result = apply_deployment(
        dir.path(),
        None,
        &identity("principal:owner"),
        &BTreeMap::new(),
        |_, _, _| {},
    )
    .await
    .unwrap();
    let DeploymentLookup::Complete { result } = result else {
        panic!("{result:?}");
    };
    assert_eq!(result.authority.kind, AuthorityKind::AuthenticatedIdentity);
    assert_eq!(result.authority.actor.as_deref(), Some("principal:owner"));
    let GraphDeploymentResult::Schema {
        result: SchemaApplySettlement::Committed { commit, .. },
    } = &result.graphs["knowledge"]
    else {
        panic!("{:?}", result.graphs);
    };
    assert_eq!(commit.actor_id.as_deref(), Some("principal:owner"));
}

#[tokio::test]
async fn authenticated_bootstrap_is_explicit_exact_and_single_use() {
    let dir = crate::tests::identity_fixture();
    let error = apply_deployment(
        dir.path(),
        None,
        &identity("principal:owner"),
        &BTreeMap::new(),
        |_, _, _| {},
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "bootstrap_authority_required");
    assert!(!dir.path().join(CLUSTER_STATE_FILE).exists());
    let capability = DeploymentCaller::AuthenticatedIdentity(
        IdentityAuthorization::bootstrap_config_dir("principal:owner", dir.path()).unwrap(),
    );
    let schema = fs::read_to_string(dir.path().join("people.pg")).unwrap();
    fs::write(
        dir.path().join("people.pg"),
        schema.replace("age: I32?", "age: I32?\n  extra: String?"),
    )
    .unwrap();
    let error = apply_deployment(
        dir.path(),
        None,
        &capability,
        &BTreeMap::new(),
        |_, _, _| {},
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "bootstrap_authority_mismatch");
    assert!(!dir.path().join(CLUSTER_STATE_FILE).exists());
    fs::write(dir.path().join("people.pg"), schema).unwrap();
    let applied = apply_deployment(
        dir.path(),
        None,
        &capability,
        &BTreeMap::new(),
        |_, _, _| {},
    )
    .await
    .unwrap();
    let DeploymentLookup::Complete { result } = applied else {
        panic!("{applied:?}");
    };
    assert!(result.converged);
    assert_eq!(result.authority.kind, AuthorityKind::AuthenticatedIdentity);
    assert_eq!(result.authority.actor.as_deref(), Some("principal:owner"));
    unlock(dir.path().to_str().unwrap()).await;
    let before = fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap();
    let error = apply_deployment(
        dir.path(),
        None,
        &capability,
        &BTreeMap::new(),
        |_, _, _| {},
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "bootstrap_already_initialized");
    assert_eq!(
        fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        before
    );
    assert!(
        deployment_status(dir.path().to_str().unwrap(), None, &owner())
            .await
            .unwrap()
            .lock_id
            .is_none()
    );
}

#[cfg(unix)]
#[tokio::test]
async fn new_graph_cannot_escape_cluster_root_through_symlink() {
    let dir = crate::tests::identity_fixture();
    let root = dir.path().to_str().unwrap();
    bootstrap(dir.path()).await;
    unlock(root).await;
    let external = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(external.path(), dir.path().join("graphs/second.omni")).unwrap();
    add_second_graph(dir.path());
    let before = fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap();
    let error = apply_deployment(dir.path(), None, &owner(), &BTreeMap::new(), |_, _, _| {})
        .await
        .unwrap_err();
    assert_eq!(error.code, "cluster_graph_root_mismatch");
    assert_eq!(
        fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        before
    );
    assert_eq!(std::fs::read_dir(external.path()).unwrap().count(), 0);
    assert!(
        deployment_status(root, None, &owner())
            .await
            .unwrap()
            .lock_id
            .is_none()
    );
}

#[test]
fn remote_capture_does_not_open_server_storage() {
    let dir = crate::tests::identity_fixture();
    let config_path = dir.path().join(CLUSTER_CONFIG_FILE);
    let config = fs::read_to_string(&config_path).unwrap();
    let missing = dir.path().join("server-only-storage");
    let advertised = format!("file://{}", missing.display());
    fs::write(&config_path, format!("storage: {advertised}\n{config}")).unwrap();
    assert!(!missing.exists());
    let captured = capture_deployment_for_server(dir.path(), &BTreeMap::new(), &advertised)
        .expect("remote capture must not canonicalize or open the server's filesystem");
    assert_eq!(captured.canonical_root(), advertised);
    assert!(!missing.exists());
    assert!(!dir.path().join(CLUSTER_STATE_FILE).exists());
    // Direct capture still requires a real canonical root on its own host.
    assert_eq!(
        capture_deployment(dir.path(), &BTreeMap::new())
            .unwrap_err()
            .code,
        "storage_root_invalid"
    );
    assert_eq!(
        capture_deployment_for_server(
            dir.path(),
            &BTreeMap::new(),
            "file:///different-server-root"
        )
        .unwrap_err()
        .code,
        "deployment_input_invalid"
    );

    // Declared paths never resolve against the client's current directory.
    fs::write(
        &config_path,
        format!("storage: ./server-only-storage\n{config}"),
    )
    .unwrap();
    assert_eq!(
        capture_deployment_for_server(dir.path(), &BTreeMap::new(), &advertised)
            .unwrap_err()
            .code,
        "storage_root_invalid"
    );

    // An omitted storage declaration explicitly selects the authenticated
    // server. S3/Azure capture needs no local backend or cloud credentials.
    fs::write(&config_path, &config).unwrap();
    let captured =
        capture_deployment_for_server(dir.path(), &BTreeMap::new(), &advertised).unwrap();
    assert_eq!(captured.canonical_root(), advertised);
    for root in [
        "s3://capture-only-bucket/cluster",
        "az://capture-only-container/cluster",
    ] {
        fs::write(&config_path, format!("storage: {root}/\n{config}")).unwrap();
        let captured = capture_deployment_for_server(dir.path(), &BTreeMap::new(), root).unwrap();
        assert_eq!(captured.canonical_root(), root);
    }

    // Existing local roots produce identical frozen input across executors.
    let local_root = format!("file://{}", fs::canonicalize(dir.path()).unwrap().display());
    fs::write(&config_path, format!("storage: {local_root}\n{config}")).unwrap();
    let direct = capture_deployment(dir.path(), &BTreeMap::new()).unwrap();
    let remote = capture_deployment_for_server(dir.path(), &BTreeMap::new(), &local_root).unwrap();
    assert_eq!(
        serde_json::to_value(direct).unwrap(),
        serde_json::to_value(remote).unwrap()
    );
    #[cfg(unix)]
    {
        let alias = dir.path().join("client-storage-alias");
        std::os::unix::fs::symlink(dir.path(), &alias).unwrap();
        fs::write(
            &config_path,
            format!("storage: {}\n{config}", alias.display()),
        )
        .unwrap();
        assert_eq!(
            capture_deployment(dir.path(), &BTreeMap::new())
                .unwrap()
                .canonical_root(),
            local_root
        );
        assert_eq!(
            capture_deployment_for_server(dir.path(), &BTreeMap::new(), &local_root)
                .unwrap_err()
                .code,
            "deployment_input_invalid"
        );
    }
}
