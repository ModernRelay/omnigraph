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
    let result = apply_deployment(dir, None, &owner(), |_, _, _| {})
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
    let policy_path = dir.path().join("base.policy.yaml");
    let policy_source = fs::read_to_string(&policy_path).unwrap();
    fs::write(
        &policy_path,
        format!("{policy_source}\n# new policy revision\n"),
    )
    .unwrap();
    let captured = capture_deployment(dir.path()).unwrap();
    let before = fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap();
    let preview = prepare_deployment_preview(&captured, &admission, &owner())
        .await
        .unwrap()
        .serving;
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
            .map(|policy| &policy.applies_to)
            .collect::<Vec<_>>(),
        snapshot
            .policies
            .iter()
            .map(|policy| &policy.applies_to)
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
async fn held_server_owner_applies_twice_and_retains_exact_achieved_receipts() {
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
        let bundle = capture_deployment(dir.path()).unwrap();
        let preview = prepare_deployment_preview(&bundle, &admission, &owner())
            .await
            .unwrap();
        assert_eq!(preview.affected_graphs, ["knowledge"]);
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
        assert_eq!(contracts["knowledge"], db.schema_contract_digest());
        let before = fs::read(dir.path().join("__cluster/state.json")).unwrap();
        let status = deployment_status(root, Some(&result.id), &owner())
            .await
            .unwrap();
        let Some(DeploymentLookup::Complete { result: observed }) = status.lookup else {
            panic!("completed receipt missing");
        };
        assert_eq!(
            serde_json::to_value(&observed).unwrap(),
            serde_json::to_value(&result).unwrap()
        );
        assert_eq!(
            fs::read(dir.path().join("__cluster/state.json")).unwrap(),
            before
        );
    }
    // Membership captured at server boot is not current deployment authority:
    // create and delete a later graph under this same lifetime writer.
    add_second_graph(dir.path());
    let bundle = capture_deployment(dir.path()).unwrap();
    let mut started = false;
    let created = apply_captured_deployment(
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
    let DeploymentLookup::Complete { result: created } = created else {
        panic!("not complete")
    };
    let GraphDeploymentResult::Created { contract, .. } = &created.graphs["second"] else {
        panic!("not created")
    };
    edit_config(dir.path(), |config| {
        config["graphs"]
            .as_mapping_mut()
            .unwrap()
            .remove(serde_yaml::Value::from("second"));
        config["policies"]["base"]["applies_to"] = serde_yaml::from_str("[knowledge]").unwrap();
    });
    let bundle = capture_deployment(dir.path()).unwrap();
    let deleted = apply_captured_deployment(
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
    assert!(
        matches!(deleted, DeploymentLookup::Complete { result } if result.converged && matches!(&result.graphs["second"], GraphDeploymentResult::Deleted { contract: old } if old == contract))
    );
    assert!(!dir.path().join("graphs/second.omni").exists());
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
    let mut bundle = capture_deployment(dir.path()).unwrap();
    let original = serde_json::to_value(&bundle).unwrap();
    for field in [
        "options",
        "adopt_graphs",
        "recreate_graphs",
        "schema_corrections",
        "repair_catalog",
        "delete_graphs",
    ] {
        let mut legacy = original.clone();
        legacy[field] = serde_json::json!({});
        let error = serde_json::from_value::<CapturedDeployment>(legacy).unwrap_err();
        assert!(
            error.to_string().contains("unknown field"),
            "{field}: {error}"
        );
    }
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
    let error = apply_deployment(dir.path(), None, &owner(), |_, _, _| {})
        .await
        .unwrap_err();
    assert_eq!(error.code, "graph_root_exists");
    assert!(
        deployment_status(root, None, &owner())
            .await
            .unwrap()
            .lock_id
            .is_none()
    );

    // A lost ledger cannot silently adopt existing graphs or initialize a
    // replacement ledger over their identities.
    let knowledge = graph_state(dir.path(), "knowledge").await;
    let second = graph_state(dir.path(), "second").await;
    fs::remove_file(dir.path().join(CLUSTER_STATE_FILE)).unwrap();
    let captured = capture_deployment(dir.path()).unwrap();
    assert_eq!(
        preflight_deployment(&captured, &owner())
            .await
            .unwrap_err()
            .code,
        "graph_root_exists"
    );
    assert_eq!(
        apply_deployment(dir.path(), None, &owner(), |_, _, _| {})
            .await
            .unwrap_err()
            .code,
        "graph_root_exists"
    );
    assert!(!dir.path().join(CLUSTER_STATE_FILE).exists());
    assert!(!dir.path().join(CLUSTER_LOCK_FILE).exists());
    assert_eq!(graph_state(dir.path(), "knowledge").await, knowledge);
    assert_eq!(graph_state(dir.path(), "second").await, second);
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
        |_, _, _| {},
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "policy_denied");
    assert_eq!(
        fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        before
    );
    // Graph removal still requires the existing schema capability in addition
    // to cluster management. A candidate policy cannot grant it retroactively.
    edit_config(dir.path(), |config| {
        config["graphs"] = serde_yaml::from_str("{}").unwrap();
        config["policies"]
            .as_mapping_mut()
            .unwrap()
            .remove(serde_yaml::Value::from("base"));
    });
    let graph_before = graph_state(dir.path(), "knowledge").await;
    let error = apply_deployment(
        dir.path(),
        None,
        &identity("principal:collaborator"),
        |_, _, _| {},
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "policy_denied");
    assert_eq!(
        fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        before
    );
    assert_eq!(graph_state(dir.path(), "knowledge").await, graph_before);
    assert!(!dir.path().join(CLUSTER_LOCK_FILE).exists());
    for actor in [None, Some("principal:collaborator".to_owned())] {
        let error = apply_deployment(
            dir.path(),
            None,
            &DeploymentCaller::storage_owner(actor),
            |_, _, _| {},
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "policy_denied");
        assert_eq!(
            fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
            before
        );
        assert_eq!(graph_state(dir.path(), "knowledge").await, graph_before);
        assert!(!dir.path().join(CLUSTER_LOCK_FILE).exists());
    }
    // An already absent managed root remains removable by an authorized actor,
    // including through the observational planning path.
    fs::remove_dir_all(dir.path().join("graphs/knowledge.omni")).unwrap();
    let identity = IdentityAuthorization::authenticated("principal:owner").unwrap();
    let plan = plan_config_dir_authorized(dir.path(), &identity).await;
    assert!(plan.plan.ok, "{plan:?}");
    let result = apply_deployment(
        dir.path(),
        None,
        &DeploymentCaller::AuthenticatedIdentity(identity),
        |_, _, _| {},
    )
    .await
    .unwrap();
    assert!(
        matches!(result, DeploymentLookup::Complete { result } if matches!(&result.graphs["knowledge"], GraphDeploymentResult::Deleted { contract } if contract == &graph_before.0))
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
    let result = apply_deployment(dir.path(), None, &identity("principal:owner"), |_, _, _| {})
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
    let error = apply_deployment(dir.path(), None, &identity("principal:owner"), |_, _, _| {})
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
    let error = apply_deployment(dir.path(), None, &capability, |_, _, _| {})
        .await
        .unwrap_err();
    assert_eq!(error.code, "bootstrap_authority_mismatch");
    assert!(!dir.path().join(CLUSTER_STATE_FILE).exists());
    fs::write(dir.path().join("people.pg"), schema).unwrap();
    // A pristine v2 ledger may already exist after initialization lost its
    // caller before acceptance. Repeated observed plans consume no authority.
    let store = ClusterStore::for_config_dir(dir.path());
    let pristine = execution::empty_ledger();
    store
        .write_state(&pristine, None, &mut store.observations())
        .await
        .unwrap();
    let before = fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap();
    let DeploymentCaller::AuthenticatedIdentity(bootstrap_identity) = &capability else {
        unreachable!();
    };
    let mut first_authorization = None;
    for _ in 0..2 {
        let planned = plan_config_dir_authorized(dir.path(), bootstrap_identity).await;
        assert!(planned.plan.ok, "{:?}", planned.plan.diagnostics);
        assert_eq!(planned.plan.authority, LedgerAuthority::Observed);
        assert!(!planned.plan.state_observations.lock_acquired);
        let authorization = planned.authorization.unwrap();
        assert_eq!(authorization.state_revision, 1);
        assert!(authorization.bootstrap);
        if let Some(first) = &first_authorization {
            assert_eq!(&authorization, first);
        } else {
            first_authorization = Some(authorization);
        }
        assert_eq!(
            fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
            before
        );
        assert!(!dir.path().join(CLUSTER_LOCK_FILE).exists());
        assert!(!dir.path().join("graphs/knowledge.omni").exists());
    }

    let applied = apply_deployment(dir.path(), None, &capability, |_, _, _| {})
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
    let error = apply_deployment(dir.path(), None, &capability, |_, _, _| {})
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
async fn graph_lifecycle_cannot_escape_cluster_root_through_symlink() {
    for deleting in [false, true] {
        let dir = crate::tests::identity_fixture();
        let root = dir.path().to_str().unwrap();
        if deleting {
            add_second_graph(dir.path());
        }
        bootstrap(dir.path()).await;
        unlock(root).await;
        let external = tempfile::tempdir().unwrap();
        fs::write(external.path().join("keep"), b"external").unwrap();
        let graph_root = dir.path().join("graphs/second.omni");
        if deleting {
            fs::remove_dir_all(&graph_root).unwrap();
            edit_config(dir.path(), |config| {
                config["graphs"]
                    .as_mapping_mut()
                    .unwrap()
                    .remove(serde_yaml::Value::from("second"));
                config["policies"]["base"]["applies_to"] =
                    serde_yaml::from_str("[knowledge]").unwrap();
            });
        } else {
            add_second_graph(dir.path());
        }
        std::os::unix::fs::symlink(external.path(), &graph_root).unwrap();
        let before = fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap();
        let error = apply_deployment(dir.path(), None, &owner(), |_, _, _| {})
            .await
            .unwrap_err();
        assert_eq!(error.code, "cluster_graph_root_mismatch");
        assert_eq!(
            fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
            before
        );
        assert_eq!(fs::read(external.path().join("keep")).unwrap(), b"external");
        assert!(
            deployment_status(root, None, &owner())
                .await
                .unwrap()
                .lock_id
                .is_none()
        );
    }
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
    let captured = capture_deployment_for_server(dir.path(), &advertised)
        .expect("remote capture must not canonicalize or open the server's filesystem");
    assert_eq!(captured.canonical_root(), advertised);
    assert!(!missing.exists());
    assert!(!dir.path().join(CLUSTER_STATE_FILE).exists());
    // Direct capture still requires a real canonical root on its own host.
    assert_eq!(
        capture_deployment(dir.path()).unwrap_err().code,
        "storage_root_invalid"
    );
    assert_eq!(
        capture_deployment_for_server(dir.path(), "file:///different-server-root")
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
        capture_deployment_for_server(dir.path(), &advertised)
            .unwrap_err()
            .code,
        "storage_root_invalid"
    );

    // An omitted storage declaration explicitly selects the authenticated
    // server. S3/Azure capture needs no local backend or cloud credentials.
    fs::write(&config_path, &config).unwrap();
    let captured = capture_deployment_for_server(dir.path(), &advertised).unwrap();
    assert_eq!(captured.canonical_root(), advertised);
    for root in [
        "s3://capture-only-bucket/cluster",
        "az://capture-only-container/cluster",
    ] {
        fs::write(&config_path, format!("storage: {root}/\n{config}")).unwrap();
        let captured = capture_deployment_for_server(dir.path(), root).unwrap();
        assert_eq!(captured.canonical_root(), root);
    }

    // Existing local roots produce identical frozen input across executors.
    let local_root = format!("file://{}", fs::canonicalize(dir.path()).unwrap().display());
    fs::write(&config_path, format!("storage: {local_root}\n{config}")).unwrap();
    let direct = capture_deployment(dir.path()).unwrap();
    let remote = capture_deployment_for_server(dir.path(), &local_root).unwrap();
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
            capture_deployment(dir.path()).unwrap().canonical_root(),
            local_root
        );
        assert_eq!(
            capture_deployment_for_server(dir.path(), &local_root)
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

async fn graph_state(dir: &Path, graph: &str) -> (omnigraph::db::SchemaContractDigest, u64) {
    let db = Omnigraph::open_read_only(dir.join(format!("graphs/{graph}.omni")).to_str().unwrap())
        .await
        .unwrap();
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    (
        db.schema_contract_digest(),
        snapshot.graph_manifest_version(),
    )
}

#[tokio::test]
async fn current_policy_authorizes_policy_edits_rebinding_and_removal() {
    let dir = crate::tests::identity_fixture();
    let root = dir.path().to_str().unwrap();
    bootstrap(dir.path()).await;
    unlock(root).await;
    let original = graph_state(dir.path(), "knowledge").await;
    let graph_policy = dir.path().join("base.policy.yaml");
    let source = fs::read_to_string(&graph_policy)
        .unwrap()
        .replace("principal:reader]", "principal:reader, principal:alice]");
    fs::write(&graph_policy, &source).unwrap();
    let applied = apply_deployment(dir.path(), None, &identity("principal:owner"), |_, _, _| {})
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
    assert_eq!(graph_state(dir.path(), "knowledge").await, original);
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
    bootstrap(dir.path()).await;
    unlock(root).await;
    edit_config(dir.path(), |config| {
        config["policies"]
            .as_mapping_mut()
            .unwrap()
            .remove(serde_yaml::Value::from("replacement"));
    });
    bootstrap(dir.path()).await;
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
    assert_eq!(graph_state(dir.path(), "knowledge").await, original);
    unlock(root).await;
    // Current management policy authorizes revoking the author itself. A
    // subsequent candidate restoring access cannot authorize its own request.
    let management = dir.path().join("management.policy.yaml");
    let source = fs::read_to_string(&management).unwrap();
    fs::write(&management, source.replace("principal:owner, ", "")).unwrap();
    apply_deployment(dir.path(), None, &identity("principal:owner"), |_, _, _| {})
        .await
        .unwrap();
    unlock(root).await;
    fs::write(&management, source).unwrap();
    assert_eq!(
        apply_deployment(dir.path(), None, &identity("principal:owner"), |_, _, _| {})
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
    let original = graph_state(dir.path(), "knowledge").await;
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
        bootstrap(dir.path()).await;
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
    bootstrap(dir.path()).await;
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
    assert_eq!(graph_state(dir.path(), "knowledge").await, original);
}

#[tokio::test]
async fn graph_removal_deletes_exact_managed_root_and_retains_peers() {
    for initial_root in ["present", "absent", "foreign"] {
        let already_absent = initial_root == "absent";
        let dir = crate::tests::identity_fixture();
        let root = dir.path().to_str().unwrap();
        add_second_graph(dir.path());
        bootstrap(dir.path()).await;
        unlock(root).await;
        let (contract, _) = graph_state(dir.path(), "second").await;
        let peer = graph_state(dir.path(), "knowledge").await;
        let removed_root = dir.path().join("graphs/second.omni");
        let neighboring_prefix = dir.path().join("graphs/second.omni-other");
        fs::create_dir_all(&neighboring_prefix).unwrap();
        fs::write(neighboring_prefix.join("keep"), "peer").unwrap();
        let external_blob = dir.path().join("external-blob.bin");
        fs::write(&external_blob, b"shared external bytes").unwrap();
        if already_absent {
            fs::remove_dir_all(&removed_root).unwrap();
        }
        let ledger = fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap();
        edit_config(dir.path(), |config| {
            config["graphs"]
                .as_mapping_mut()
                .unwrap()
                .remove(serde_yaml::Value::from("second"));
            config["policies"]["base"]["applies_to"] = serde_yaml::from_str("[knowledge]").unwrap();
        });
        let captured = capture_deployment(dir.path()).unwrap();
        if initial_root == "foreign" {
            fs::remove_dir_all(&removed_root).unwrap();
            Omnigraph::init(removed_root.to_str().unwrap(), crate::tests::SCHEMA)
                .await
                .unwrap();
            let foreign = graph_state(dir.path(), "second").await;
            assert_ne!(foreign.0, contract);
            assert_eq!(
                preflight_deployment(&captured, &owner())
                    .await
                    .unwrap_err()
                    .code,
                "applied_schema_drift"
            );
            assert_eq!(
                apply_deployment(dir.path(), None, &owner(), |_, _, _| {})
                    .await
                    .unwrap_err()
                    .code,
                "applied_schema_drift"
            );
            assert_eq!(
                fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
                ledger
            );
            assert_eq!(graph_state(dir.path(), "second").await, foreign);
            assert_eq!(graph_state(dir.path(), "knowledge").await, peer);
            continue;
        }
        preflight_deployment(&captured, &owner()).await.unwrap();
        assert_eq!(
            fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
            ledger
        );
        assert_eq!(removed_root.exists(), !already_absent);
        let result = bootstrap(dir.path()).await;
        assert!(
            matches!(&result.graphs["second"], GraphDeploymentResult::Deleted { contract: deleted } if deleted == &contract)
        );
        assert!(!removed_root.exists());
        assert_eq!(graph_state(dir.path(), "knowledge").await, peer);
        assert_eq!(fs::read(neighboring_prefix.join("keep")).unwrap(), b"peer");
        assert_eq!(fs::read(&external_blob).unwrap(), b"shared external bytes");
        let state: ClusterState =
            serde_json::from_slice(&fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap())
                .unwrap();
        assert!(
            !state
                .applied_revision
                .schema_contracts
                .unwrap()
                .contains_key("second")
        );
        assert!(state.applied_revision.resources.keys().all(|address| !matches!(resource_kind(address), ResourceKind::Graph(ref id) | ResourceKind::Schema(ref id) if id == "second")));
        assert!(
            state
                .resource_statuses
                .keys()
                .all(|address| !address.ends_with(".second"))
        );
        assert_eq!(
            read_serving_snapshot_from_storage(root)
                .await
                .unwrap()
                .graphs
                .len(),
            1
        );
        // Terminal lookup must never issue another purge, even if a new object
        // subsequently appears at the old path.
        fs::create_dir(&removed_root).unwrap();
        fs::write(removed_root.join("new-owner"), b"keep").unwrap();
        let lookup = reconcile_deployment(root, &result.id, false, &owner())
            .await
            .unwrap();
        assert!(matches!(lookup, DeploymentLookup::Complete { .. }));
        assert_eq!(fs::read(removed_root.join("new-owner")).unwrap(), b"keep");
    }
}

#[tokio::test]
async fn missing_managed_graph_refuses_without_recreating_storage() {
    let dir = crate::tests::identity_fixture();
    let root = dir.path().to_str().unwrap();
    bootstrap(dir.path()).await;
    unlock(root).await;
    let ledger = fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap();
    fs::remove_dir_all(dir.path().join("graphs/knowledge.omni")).unwrap();
    fs::write(
        dir.path().join("people.pg"),
        format!("{}\nnode Extra {{ value: String }}\n", crate::tests::SCHEMA),
    )
    .unwrap();
    let captured = capture_deployment(dir.path()).unwrap();
    assert_eq!(
        preflight_deployment(&captured, &owner())
            .await
            .unwrap_err()
            .code,
        "graph_unavailable"
    );
    assert_eq!(
        apply_deployment(dir.path(), None, &owner(), |_, _, _| {})
            .await
            .unwrap_err()
            .code,
        "graph_unavailable"
    );
    assert_eq!(
        fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        ledger
    );
    assert!(!dir.path().join("graphs/knowledge.omni").exists());
    assert!(!dir.path().join(CLUSTER_LOCK_FILE).exists());
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
    let captured = capture_deployment(dir.path()).unwrap();
    preflight_deployment(&captured, &owner()).await.unwrap();
    let result = bootstrap(dir.path()).await;
    assert_eq!(result.graphs.keys().collect::<Vec<_>>(), vec!["knowledge"]);
    assert!(!dir.path().join("graphs/second.omni").exists());
}

#[tokio::test]
async fn corrupt_catalog_payloads_refuse_without_repair_or_candidate_authority() {
    let dir = crate::tests::identity_fixture();
    let root = dir.path().to_str().unwrap();
    bootstrap(dir.path()).await;
    unlock(root).await;
    let original = graph_state(dir.path(), "knowledge").await;
    let ledger = fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap();
    let store = ClusterStore::for_config_dir(dir.path());
    let (state, _) = execution::read_existing(&store).await.unwrap();
    let address = "policy.management";
    let digest = &state.applied_revision.resources[address].digest;
    let relative = ClusterStore::payload_relative(&resource_kind(address), digest).unwrap();
    let payload = dir.path().join(relative);
    let trusted = fs::read(&payload).unwrap();
    fs::write(&payload, [0xff, 0xfe]).unwrap();
    for caller in [owner(), identity("principal:owner")] {
        assert!(
            apply_deployment(dir.path(), None, &caller, |_, _, _| {})
                .await
                .is_err()
        );
        assert_eq!(fs::read(&payload).unwrap(), [0xff, 0xfe]);
        assert_eq!(
            fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
            ledger
        );
        assert!(!dir.path().join(CLUSTER_LOCK_FILE).exists());
    }
    // Restore authoritative bytes in the test fixture, then corrupt an applied
    // query. Even replacing/deleting that query is not an implicit repair path.
    fs::write(&payload, trusted).unwrap();
    let address = "query.knowledge.find_person";
    let digest = &state.applied_revision.resources[address].digest;
    let relative = ClusterStore::payload_relative(&resource_kind(address), digest).unwrap();
    let payload = dir.path().join(relative);
    fs::write(&payload, "corrupt").unwrap();
    let query_path = dir.path().join("people.gq");
    let source = fs::read_to_string(&query_path).unwrap();
    fs::write(
        &query_path,
        source.replace("return { $p.name, $p.age }", "return { $p.name }"),
    )
    .unwrap();
    for remove in [false, true] {
        if remove {
            edit_config(dir.path(), |config| {
                config["graphs"]["knowledge"]
                    .as_mapping_mut()
                    .unwrap()
                    .remove(serde_yaml::Value::from("queries"));
            });
        }
        let captured = capture_deployment(dir.path()).unwrap();
        assert_eq!(
            preflight_deployment(&captured, &owner())
                .await
                .unwrap_err()
                .code,
            "catalog_payload_invalid"
        );
        assert_eq!(
            apply_deployment(dir.path(), None, &owner(), |_, _, _| {})
                .await
                .unwrap_err()
                .code,
            "catalog_payload_invalid"
        );
        assert_eq!(fs::read(&payload).unwrap(), b"corrupt");
        assert_eq!(
            fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
            ledger
        );
        assert!(!dir.path().join(CLUSTER_LOCK_FILE).exists());
    }
    assert_eq!(graph_state(dir.path(), "knowledge").await, original);
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
    let result = apply_deployment(dir.path(), None, &actor, |_, _, _| {})
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
