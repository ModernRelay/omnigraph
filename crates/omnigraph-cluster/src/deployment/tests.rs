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
    let captured = capture_deployment_with_options(
        dir.path(),
        &DeploymentOptions {
            repair_catalog: BTreeSet::from(["policy.base".into()]),
            ..DeploymentOptions::default()
        },
    )
    .unwrap();
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
    assert_eq!(error.code, "graph_adoption_required");
    assert!(
        deployment_status(root, None, &owner())
            .await
            .unwrap()
            .lock_id
            .is_none()
    );

    // Loss of the ledger is still an explicit adoption, never an implicit
    // replacement. A stale confirmation must not create an empty new ledger.
    let knowledge = confirmation(dir.path(), "knowledge").await;
    let second = confirmation(dir.path(), "second").await;
    fs::remove_file(dir.path().join(CLUSTER_STATE_FILE)).unwrap();
    let mut stale = second.clone();
    stale.graph_manifest_version += 1;
    let mut options = DeploymentOptions {
        adopt_graphs: BTreeMap::from([
            ("knowledge".into(), knowledge.clone()),
            ("second".into(), stale),
        ]),
        ..DeploymentOptions::default()
    };
    let captured = capture_deployment_with_options(dir.path(), &options).unwrap();
    assert_eq!(
        preflight_deployment(&captured, &owner())
            .await
            .unwrap_err()
            .code,
        "graph_lifecycle_confirmation_mismatch"
    );
    assert_eq!(
        apply_deployment_with_options(dir.path(), None, &owner(), &options, |_, _, _| {})
            .await
            .unwrap_err()
            .code,
        "graph_lifecycle_confirmation_mismatch"
    );
    assert!(!dir.path().join(CLUSTER_STATE_FILE).exists());
    assert!(!dir.path().join(CLUSTER_LOCK_FILE).exists());
    options.adopt_graphs.insert("second".into(), second.clone());
    let adopted = apply_options(dir.path(), &options).await;
    assert!(
        adopted
            .graphs
            .values()
            .all(|result| matches!(result, GraphDeploymentResult::Adopted { .. }))
    );
    assert_eq!(confirmation(dir.path(), "knowledge").await, knowledge);
    assert_eq!(confirmation(dir.path(), "second").await, second);
}

fn identity(actor: &str) -> DeploymentCaller {
    DeploymentCaller::AuthenticatedIdentity(IdentityAuthorization::authenticated(actor).unwrap())
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

fn edit_config(dir: &Path, edit: impl FnOnce(&mut serde_yaml::Value)) {
    let path = dir.join(CLUSTER_CONFIG_FILE);
    let mut value = serde_yaml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
    edit(&mut value);
    fs::write(path, serde_yaml::to_string(&value).unwrap()).unwrap();
}

async fn confirmation(dir: &Path, graph: &str) -> GraphLifecycleConfirmation {
    let db = Omnigraph::open_read_only(dir.join(format!("graphs/{graph}.omni")).to_str().unwrap())
        .await
        .unwrap();
    execution::graph_confirmation(&db).await.unwrap()
}

async fn apply_options(dir: &Path, options: &DeploymentOptions) -> DeploymentResult {
    let applied = apply_deployment_with_options(dir, None, &owner(), options, |_, _, _| {})
        .await
        .unwrap();
    let DeploymentLookup::Complete { result } = applied else {
        panic!("{applied:?}")
    };
    assert!(result.converged, "{result:?}");
    result
}

#[tokio::test]
async fn current_policy_authorizes_policy_edits_rebinding_and_removal() {
    let dir = crate::tests::identity_fixture();
    let root = dir.path().to_str().unwrap();
    bootstrap(dir.path()).await;
    unlock(root).await;
    let original = confirmation(dir.path(), "knowledge").await;
    let graph_policy = dir.path().join("base.policy.yaml");
    let source = fs::read_to_string(&graph_policy)
        .unwrap()
        .replace("principal:reader]", "principal:reader, principal:alice]");
    fs::write(&graph_policy, &source).unwrap();
    let applied = apply_deployment(
        dir.path(),
        None,
        &identity("principal:owner"),
        &BTreeMap::new(),
        |_, _, _| {},
    )
    .await
    .unwrap();
    assert!(matches!(applied, DeploymentLookup::Complete { result } if result.converged));
    let store = ClusterStore::for_config_dir(dir.path());
    let (state, _) = execution::read_existing(&store).await.unwrap();
    let policies = crate::authorization::AppliedPolicies::load(&store, &state)
        .await
        .unwrap();
    policies
        .check_graph(
            "principal:alice",
            "knowledge",
            omnigraph_policy::PolicyAction::Read,
        )
        .unwrap();
    assert_eq!(confirmation(dir.path(), "knowledge").await, original);
    unlock(root).await;
    fs::write(dir.path().join("replacement.policy.yaml"), source).unwrap();
    edit_config(dir.path(), |config| {
        config["policies"]
            .as_mapping_mut()
            .unwrap()
            .remove(serde_yaml::Value::from("base"));
        config["policies"]["replacement"] =
            serde_yaml::from_str("file: ./replacement.policy.yaml\napplies_to: [knowledge]\n")
                .unwrap();
    });
    apply_options(dir.path(), &DeploymentOptions::default()).await;
    unlock(root).await;
    edit_config(dir.path(), |config| {
        config["policies"]
            .as_mapping_mut()
            .unwrap()
            .remove(serde_yaml::Value::from("replacement"));
    });
    apply_options(dir.path(), &DeploymentOptions::default()).await;
    let (state, _) = execution::read_existing(&store).await.unwrap();
    assert!(!state.applied_revision.resources.contains_key("policy.base"));
    assert!(
        !state
            .applied_revision
            .resources
            .contains_key("policy.replacement")
    );
    assert!(
        crate::authorization::AppliedPolicies::load(&store, &state)
            .await
            .unwrap()
            .graph("knowledge")
            .is_none()
    );
    assert_eq!(confirmation(dir.path(), "knowledge").await, original);
    unlock(root).await;
    // Current management policy authorizes revoking the author itself. A
    // subsequent candidate restoring access cannot authorize its own request.
    let management = dir.path().join("management.policy.yaml");
    let source = fs::read_to_string(&management).unwrap();
    fs::write(&management, source.replace("principal:owner, ", "")).unwrap();
    apply_deployment(
        dir.path(),
        None,
        &identity("principal:owner"),
        &BTreeMap::new(),
        |_, _, _| {},
    )
    .await
    .unwrap();
    unlock(root).await;
    fs::write(&management, source).unwrap();
    assert_eq!(
        apply_deployment(
            dir.path(),
            None,
            &identity("principal:owner"),
            &BTreeMap::new(),
            |_, _, _| {}
        )
        .await
        .unwrap_err()
        .code,
        "policy_denied"
    );
}

#[tokio::test]
async fn provider_crud_binding_and_blob_updates_preserve_graph_identity() {
    let dir = crate::tests::identity_fixture();
    let root = dir.path().to_str().unwrap();
    bootstrap(dir.path()).await;
    unlock(root).await;
    let original = confirmation(dir.path(), "knowledge").await;
    for model in ["mock:first", "mock:second"] {
        edit_config(dir.path(), |config| {
            config["providers"] = serde_yaml::from_str(&format!(
                "embedding:\n  selected:\n    kind: mock\n    model: {model}\n"
            ))
            .unwrap();
            config["graphs"]["knowledge"]["embedding_provider"] = "selected".into();
            config["graphs"]["knowledge"]["external_blobs"] = serde_yaml::from_str(
                "allow:\n  - base: s3://trusted-bucket/data/\n    scope: server_safe\n",
            )
            .unwrap();
        });
        apply_options(dir.path(), &DeploymentOptions::default()).await;
        unlock(root).await;
    }
    edit_config(dir.path(), |config| {
        config
            .as_mapping_mut()
            .unwrap()
            .remove(serde_yaml::Value::from("providers"));
        let graph = config["graphs"]["knowledge"].as_mapping_mut().unwrap();
        graph.remove(serde_yaml::Value::from("embedding_provider"));
        graph.remove(serde_yaml::Value::from("external_blobs"));
    });
    apply_options(dir.path(), &DeploymentOptions::default()).await;
    let store = ClusterStore::for_config_dir(dir.path());
    let (state, _) = execution::read_existing(&store).await.unwrap();
    assert!(
        !state
            .applied_revision
            .resources
            .keys()
            .any(|address| matches!(resource_kind(address), ResourceKind::EmbeddingProvider(_)))
    );
    assert!(
        state.applied_revision.resources["graph.knowledge"]
            .embedding_provider
            .is_none()
    );
    assert!(
        state.applied_revision.resources["graph.knowledge"]
            .external_blob_policy
            .is_none()
    );
    assert_eq!(confirmation(dir.path(), "knowledge").await, original);
}

#[tokio::test]
async fn graph_removal_requires_exact_confirmation_retains_storage_and_allows_readoption() {
    let dir = crate::tests::identity_fixture();
    let root = dir.path().to_str().unwrap();
    add_second_graph(dir.path());
    bootstrap(dir.path()).await;
    unlock(root).await;
    let original = confirmation(dir.path(), "second").await;
    let original_config = fs::read_to_string(dir.path().join(CLUSTER_CONFIG_FILE)).unwrap();
    edit_config(dir.path(), |config| {
        config["graphs"]
            .as_mapping_mut()
            .unwrap()
            .remove(serde_yaml::Value::from("second"));
        config["policies"]["base"]["applies_to"] = serde_yaml::from_str("[knowledge]").unwrap();
    });
    let err = apply_deployment(dir.path(), None, &owner(), &BTreeMap::new(), |_, _, _| {})
        .await
        .unwrap_err();
    assert_eq!(err.code, "graph_delete_confirmation_required");
    assert!(err.message.contains("graph_manifest_version"));
    let correction = DeploymentOptions {
        schema_corrections: BTreeMap::from([("second".into(), original.contract.clone())]),
        ..DeploymentOptions::default()
    };
    assert_eq!(
        apply_deployment_with_options(dir.path(), None, &owner(), &correction, |_, _, _| {})
            .await
            .unwrap_err()
            .code,
        "deployment_input_invalid"
    );
    let mut options = DeploymentOptions::default();
    let mut stale = original.clone();
    stale.graph_manifest_version += 1;
    options.delete_graphs.insert("second".into(), stale);
    assert_eq!(
        apply_deployment_with_options(dir.path(), None, &owner(), &options, |_, _, _| {})
            .await
            .unwrap_err()
            .code,
        "graph_lifecycle_confirmation_mismatch"
    );
    options
        .delete_graphs
        .insert("second".into(), original.clone());
    let removed = apply_options(dir.path(), &options).await;
    assert!(matches!(
        removed.graphs["second"],
        GraphDeploymentResult::Deleted {
            retained_storage: true,
            ..
        }
    ));
    assert_eq!(confirmation(dir.path(), "second").await, original);
    assert_eq!(
        apply_deployment_with_options(
            dir.path(),
            Some(&removed.id),
            &owner(),
            &DeploymentOptions::default(),
            |_, _, _| {}
        )
        .await
        .unwrap_err()
        .code,
        "deployment_input_mismatch"
    );
    assert_eq!(
        read_serving_snapshot_from_storage(root)
            .await
            .unwrap()
            .graphs
            .len(),
        1
    );
    unlock(root).await;
    fs::write(dir.path().join(CLUSTER_CONFIG_FILE), original_config).unwrap();
    assert_eq!(
        apply_deployment(dir.path(), None, &owner(), &BTreeMap::new(), |_, _, _| {})
            .await
            .unwrap_err()
            .code,
        "graph_adoption_required"
    );
    let options = DeploymentOptions {
        adopt_graphs: BTreeMap::from([("second".into(), original.clone())]),
        ..DeploymentOptions::default()
    };
    let adopted = apply_options(dir.path(), &options).await;
    assert!(matches!(
        adopted.graphs["second"],
        GraphDeploymentResult::Adopted { .. }
    ));
    assert_eq!(confirmation(dir.path(), "second").await, original);
    assert_eq!(
        read_serving_snapshot_from_storage(root)
            .await
            .unwrap()
            .graphs
            .len(),
        2
    );
}

#[tokio::test]
async fn missing_graph_recreation_requires_achieved_identity_and_never_overwrites_storage() {
    let dir = crate::tests::identity_fixture();
    let root = dir.path().to_str().unwrap();
    bootstrap(dir.path()).await;
    unlock(root).await;
    let original = confirmation(dir.path(), "knowledge").await;
    let options = DeploymentOptions {
        recreate_graphs: BTreeMap::from([("knowledge".into(), original.contract.clone())]),
        ..DeploymentOptions::default()
    };
    assert_eq!(
        apply_deployment_with_options(dir.path(), None, &owner(), &options, |_, _, _| {})
            .await
            .unwrap_err()
            .code,
        "graph_recreate_root_present"
    );
    fs::remove_dir_all(dir.path().join("graphs/knowledge.omni")).unwrap();
    let recreated = apply_options(dir.path(), &options).await;
    assert!(matches!(
        recreated.graphs["knowledge"],
        GraphDeploymentResult::Created { .. }
    ));
    assert_ne!(
        confirmation(dir.path(), "knowledge")
            .await
            .contract
            .schema_identity_domain,
        original.contract.schema_identity_domain
    );
}

#[tokio::test]
async fn unrelated_unavailable_graph_does_not_block_affected_graph_preflight_or_apply() {
    let dir = crate::tests::identity_fixture();
    let root = dir.path().to_str().unwrap();
    add_second_graph(dir.path());
    bootstrap(dir.path()).await;
    unlock(root).await;
    fs::remove_dir_all(dir.path().join("graphs/second.omni")).unwrap();
    let query_path = dir.path().join("people.gq");
    let source = fs::read_to_string(&query_path).unwrap();
    fs::write(
        query_path,
        source.replace("return { $p.name, $p.age }", "return { $p.name }"),
    )
    .unwrap();
    let captured = capture_deployment(dir.path(), &BTreeMap::new()).unwrap();
    preflight_deployment(&captured, &owner()).await.unwrap();
    let result = apply_options(dir.path(), &DeploymentOptions::default()).await;
    assert_eq!(result.graphs.keys().collect::<Vec<_>>(), vec!["knowledge"]);
    assert!(!dir.path().join("graphs/second.omni").exists());
}

#[tokio::test]
async fn targeted_catalog_repair_restores_exact_bytes_without_candidate_self_authorization() {
    let dir = crate::tests::identity_fixture();
    let root = dir.path().to_str().unwrap();
    bootstrap(dir.path()).await;
    unlock(root).await;
    let original = confirmation(dir.path(), "knowledge").await;
    let store = ClusterStore::for_config_dir(dir.path());
    let (state, _) = execution::read_existing(&store).await.unwrap();
    let digest = &state.applied_revision.resources["policy.management"].digest;
    let relative =
        ClusterStore::payload_relative(&ResourceKind::Policy("management".into()), digest).unwrap();
    fs::write(dir.path().join(relative), [0xff, 0xfe]).unwrap();
    let wrong_target = DeploymentOptions {
        repair_catalog: BTreeSet::from(["query.knowledge.find_person".into()]),
        ..DeploymentOptions::default()
    };
    assert!(
        apply_deployment_with_options(dir.path(), None, &owner(), &wrong_target, |_, _, _| {})
            .await
            .is_err()
    );
    let options = DeploymentOptions {
        repair_catalog: BTreeSet::from(["policy.management".into()]),
        ..DeploymentOptions::default()
    };
    let management_path = dir.path().join("management.policy.yaml");
    let trusted_source = fs::read_to_string(&management_path).unwrap();
    fs::write(
        &management_path,
        format!("{trusted_source}\n# changed policy input\n"),
    )
    .unwrap();
    assert!(
        apply_deployment_with_options(dir.path(), None, &owner(), &options, |_, _, _| {})
            .await
            .is_err()
    );
    assert!(!dir.path().join(CLUSTER_LOCK_FILE).exists());
    fs::write(&management_path, trusted_source).unwrap();
    assert!(
        apply_deployment_with_options(
            dir.path(),
            None,
            &identity("principal:owner"),
            &options,
            |_, _, _| {}
        )
        .await
        .is_err()
    );
    apply_options(dir.path(), &options).await;
    assert_eq!(confirmation(dir.path(), "knowledge").await, original);
    let (state, _) = execution::read_existing(&store).await.unwrap();
    crate::authorization::AppliedPolicies::load(&store, &state)
        .await
        .unwrap()
        .check_cluster("principal:owner")
        .unwrap();
    unlock(root).await;
    let query = "query.knowledge.find_person";
    let digest = &state.applied_revision.resources[query].digest;
    let relative = ClusterStore::payload_relative(&resource_kind(query), digest).unwrap();
    fs::write(
        dir.path().join(relative),
        vec![b'x'; crate::config::MAX_CONFIG_SOURCE_BYTES + 1],
    )
    .unwrap();
    let options = DeploymentOptions {
        repair_catalog: BTreeSet::from([query.into()]),
        ..DeploymentOptions::default()
    };
    let captured = capture_deployment_with_options(dir.path(), &options).unwrap();
    preflight_deployment(&captured, &owner()).await.unwrap();
    apply_options(dir.path(), &options).await;
    assert_eq!(confirmation(dir.path(), "knowledge").await, original);
    assert!(
        read_serving_snapshot_from_storage(root)
            .await
            .unwrap()
            .diagnostics
            .is_empty()
    );
}

#[tokio::test]
async fn configuration_management_does_not_require_unrelated_graph_data_read() {
    let dir = crate::tests::identity_fixture();
    let root = dir.path().to_str().unwrap();
    add_second_graph(dir.path());
    fs::write(
        dir.path().join("closed.policy.yaml"),
        "version: 1\nrules: []\n",
    )
    .unwrap();
    edit_config(dir.path(), |config| {
        config["policies"]["base"]["applies_to"] = serde_yaml::from_str("[knowledge]").unwrap();
        config["policies"]["closed"] =
            serde_yaml::from_str("file: ./closed.policy.yaml\napplies_to: [second]\n").unwrap();
    });
    bootstrap(dir.path()).await;
    unlock(root).await;
    let store = ClusterStore::for_config_dir(dir.path());
    let (state, _) = execution::read_existing(&store).await.unwrap();
    assert!(
        crate::authorization::AppliedPolicies::load(&store, &state)
            .await
            .unwrap()
            .check_graph(
                "principal:owner",
                "second",
                omnigraph_policy::PolicyAction::Read
            )
            .is_err()
    );
    let actor = identity("principal:owner");
    deployment_status(root, None, &actor).await.unwrap();
    let path = dir.path().join("base.policy.yaml");
    let source = fs::read_to_string(&path)
        .unwrap()
        .replace("principal:reader]", "principal:reader, principal:alice]");
    fs::write(path, source).unwrap();
    let result = apply_deployment(dir.path(), None, &actor, &BTreeMap::new(), |_, _, _| {})
        .await
        .unwrap();
    let DeploymentLookup::Complete { result } = result else {
        panic!("missing result")
    };
    assert!(result.converged);
    assert_eq!(
        result.graphs.keys().map(String::as_str).collect::<Vec<_>>(),
        ["knowledge"]
    );
    deployment_status(root, Some(&result.id), &actor)
        .await
        .unwrap();
    let (state, _) = execution::read_existing(&store).await.unwrap();
    assert!(
        crate::authorization::AppliedPolicies::load(&store, &state)
            .await
            .unwrap()
            .check_graph(
                "principal:owner",
                "second",
                omnigraph_policy::PolicyAction::Read
            )
            .is_err()
    );
}
