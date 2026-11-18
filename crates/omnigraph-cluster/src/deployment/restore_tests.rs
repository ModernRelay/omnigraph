use super::*;

#[tokio::test]
async fn full_history_restore_requires_sealed_destination_and_current_config_manager() {
    exercise_sealed_restore(false).await;
}

#[tokio::test]
async fn full_history_restore_rechecks_the_seal_when_settling_an_accepted_id() {
    exercise_sealed_restore(true).await;
}

async fn exercise_sealed_restore(interrupt_after_acceptance: bool) {
    let dir = crate::tests::identity_fixture();
    let config = dir.path().join(CLUSTER_CONFIG_FILE);
    let desired = fs::read_to_string(&config).unwrap();
    let (header, _) = desired.split_once("graphs:").unwrap();
    let empty = format!(
        "{header}graphs: {{}}\npolicies:\n  management:\n    file: ./management.policy.yaml\n    applies_to: [cluster]\n"
    );
    fs::write(&config, &empty).unwrap();
    let bootstrap = bootstrap(dir.path()).await;
    assert!(bootstrap.graphs.is_empty());
    let root = dir.path().to_str().unwrap();
    unlock(root).await;
    let admission = acquire_cluster_admission(root, ClusterAdmissionPurpose::Serve)
        .await
        .unwrap()
        .unwrap();
    let manager = identity("principal:owner");
    let before = fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap();
    let source_dir = tempfile::tempdir().unwrap();
    let source = source_dir.path().join("source.omni");
    let uri = source.to_str().unwrap();
    let original = Omnigraph::init(uri, crate::tests::SCHEMA).await.unwrap();
    let original = omnigraph::Session::from_defaults(Arc::new(original), Default::default());
    original
        .load(
            "main",
            r#"{"type":"Person","data":{"name":"Before"}}"#,
            omnigraph::loader::LoadMode::Overwrite,
        )
        .await
        .unwrap();
    original.branch_create("review").await.unwrap();
    original
        .load(
            "main",
            r#"{"type":"Person","data":{"name":"After"}}"#,
            omnigraph::loader::LoadMode::Overwrite,
        )
        .await
        .unwrap();
    drop(original);
    let history = Omnigraph::open_read_only(uri)
        .await
        .unwrap()
        .verify_restored_history()
        .await
        .unwrap();
    let state: ClusterState = serde_json::from_slice(&before).unwrap();
    assert_ne!(history.branch_heads["main"], history.branch_heads["review"]);
    assert!(history.commits >= 3);
    let receipt = PreparedGraphRestore {
        version: 1,
        canonical_root: admission.canonical_root().into(),
        ledger_id: state.ledger_id.unwrap(),
        sequence: 2,
        graph: "knowledge".into(),
        object_manifest_sha256: sha256_hex(b"test object inventory"),
        history: history.clone(),
    };
    let json = serde_json::to_string(&receipt).unwrap();
    let digest = sha256_hex(json.as_bytes());
    let with_receipt = desired.replace(
        "schema: ./people.pg",
        &format!("schema: ./people.pg\n    restore_from: {digest}"),
    );
    fs::write(&config, &with_receipt).unwrap();
    let bundle = capture_deployment(dir.path()).unwrap();
    for caller in [owner(), identity("principal:reader")] {
        assert_eq!(
            prepare_deployment_preview(&bundle, &admission, &caller)
                .await
                .unwrap_err()
                .code,
            "policy_denied"
        );
    }
    assert_eq!(
        prepare_deployment_preview(&bundle, &admission, &manager)
            .await
            .unwrap_err()
            .code,
        "restore_receipt_missing"
    );
    let binding = omnigraph_storage::CapturedStorageBinding::capture(
        root,
        &omnigraph_storage::StorageDestination::Local,
    )
    .unwrap();
    assert_eq!(
        prepare_graph_restore(&binding, &json).await.unwrap(),
        digest
    );
    let target = dir.path().join("graphs/knowledge.omni");
    for entry in fs::read_dir(&source).unwrap() {
        let item = entry.unwrap();
        let dest = target.join(item.file_name());
        assert!(
            !dest.exists(),
            "source unexpectedly collides with reserved birth marker"
        );
        fs::rename(item.path(), dest).unwrap();
    }
    // The only remaining source is an empty directory; all historical reads
    // during preview, publication and serving must resolve at the destination.
    let seals = dir.path().join("__cluster/restores");
    fs::create_dir_all(&seals).unwrap();
    let seal = seals.join(format!("{digest}.json"));
    fs::write(&seal, format!("{json}\n")).unwrap();
    assert_eq!(
        prepare_deployment_preview(&bundle, &admission, &manager)
            .await
            .unwrap_err()
            .code,
        "restore_receipt_invalid"
    );
    fs::write(&seal, json).unwrap();
    for (authority, refusal) in [
        (
            dir.path()
                .join(format!("__cluster/restore-reservations/{digest}.json")),
            "restore_receipt_missing",
        ),
        (
            target.join("__init_claim.json"),
            "restore_birth_claim_mismatch",
        ),
    ] {
        let parked = authority.with_extension("parked");
        fs::rename(&authority, &parked).unwrap();
        assert_eq!(
            prepare_deployment_preview(&bundle, &admission, &manager)
                .await
                .unwrap_err()
                .code,
            refusal
        );
        fs::rename(&parked, &authority).unwrap();
    }
    fs::write(&config, &desired).unwrap();
    let ordinary = capture_deployment(dir.path()).unwrap();
    let mut attempted = false;
    assert_eq!(
        apply_captured_deployment(
            &ordinary,
            None,
            &manager,
            &admission,
            &BTreeMap::new(),
            |_, _, _| {},
            |_| panic!("ordinary creation cannot adopt the staged graph"),
            &mut attempted
        )
        .await
        .unwrap_err()
        .code,
        "graph_root_exists"
    );
    assert!(!attempted);
    fs::write(&config, &with_receipt).unwrap();
    // Simulate a stager which wrote after its seal. Even without repeating
    // an exhaustive historical scan, a new unsealed head prevents admission.
    let tampered = Omnigraph::open(target.to_str().unwrap()).await.unwrap();
    tampered.branch_create("unsealed").await.unwrap();
    assert_eq!(
        prepare_deployment_preview(&bundle, &admission, &manager)
            .await
            .unwrap_err()
            .code,
        "restore_history_mismatch"
    );
    tampered.branch_delete("unsealed").await.unwrap();
    drop(tampered);
    assert_eq!(
        fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        before
    );
    fs::write(
        dir.path().join("people.pg"),
        format!("{}\n", crate::tests::SCHEMA),
    )
    .unwrap();
    let wrong_schema = capture_deployment(dir.path()).unwrap();
    assert_eq!(
        prepare_deployment_preview(&wrong_schema, &admission, &manager)
            .await
            .unwrap_err()
            .code,
        "restore_history_mismatch"
    );
    fs::write(dir.path().join("people.pg"), crate::tests::SCHEMA).unwrap();
    let handles = BTreeMap::new();
    let plan = plan_captured_deployment(&bundle, &admission, &manager, &handles)
        .await
        .unwrap();
    assert!(plan.ok, "{:?}", plan.diagnostics);
    assert!(
        plan.changes
            .iter()
            .any(|c| c.resource == graph_address("knowledge")
                && c.restore_from.as_deref() == Some(&digest))
    );
    assert_eq!(
        fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        before
    );
    let mut effects = false;
    if interrupt_after_acceptance {
        let parked = seal.with_extension("parked");
        let id = deployment_status(root, None, &manager)
            .await
            .unwrap()
            .next_deployment_id();
        let error = apply_captured_deployment(
            &bundle,
            Some(&id),
            &manager,
            &admission,
            &handles,
            |_, _, _| {},
            |_| fs::rename(&seal, &parked).unwrap(),
            &mut effects,
        )
        .await
        .unwrap_err();
        assert!(effects);
        assert_eq!(error.code, "restore_receipt_missing");
        assert_eq!(
            deployment_status(root, Some(&id), &manager)
                .await
                .unwrap()
                .outstanding_id
                .as_deref(),
            Some(id.as_str())
        );
        // The isolated owner has finished all its I/O and the fixture performs
        // no more graph writes. Settle only the recorded ID on this same root.
        admission.release_after_settlement().await.unwrap();
        fs::rename(&parked, &seal).unwrap();
        assert_eq!(
            reconcile_deployment(root, &id, true, &owner())
                .await
                .unwrap_err()
                .code,
            "policy_denied"
        );
        let successor = identity("principal:collaborator");
        let DeploymentLookup::Complete { result } =
            reconcile_deployment(root, &id, true, &successor)
                .await
                .unwrap()
        else {
            panic!("restore not settled")
        };
        assert!(result.converged);
        assert_eq!(result.id, id);
        assert_eq!(result.authority.actor.as_deref(), Some("principal:owner"));
        assert_eq!(
            result.recovery_executors["knowledge"].actor.as_deref(),
            Some("principal:collaborator")
        );
        assert!(
            matches!(&result.graphs["knowledge"],GraphDeploymentResult::Restored { receipt_sha256, .. } if receipt_sha256 == &digest)
        );
        return;
    }
    let DeploymentLookup::Complete { result } = apply_captured_deployment(
        &bundle,
        None,
        &manager,
        &admission,
        &handles,
        |_, _, _| {},
        |_| {},
        &mut effects,
    )
    .await
    .unwrap() else {
        panic!("restore not complete")
    };
    assert!(effects && result.converged);
    assert!(
        matches!(&result.graphs["knowledge"], GraphDeploymentResult::Restored { receipt_sha256, contract, .. } if receipt_sha256 == &digest && contract == &history.schema)
    );
    assert_eq!(result.authority.actor.as_deref(), Some("principal:owner"));
    let serving = read_deployment_serving_snapshot(root, &["knowledge".into()])
        .await
        .unwrap();
    assert_eq!(serving.applied_graphs, ["knowledge"]);
    assert_eq!(
        serving.graphs[0].root.canonicalize().unwrap(),
        target.canonicalize().unwrap()
    );
    assert_eq!(serving.graph_policy("knowledge").unwrap().name, "base");
    assert_eq!(serving.queries[0].name, "find_person");
    let reopened = Omnigraph::open_read_only(serving.graphs[0].root.to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(reopened.verify_restored_history().await.unwrap(), history);
    drop(reopened);
    let mut repeated_effects = false;
    assert!(matches!(
        apply_captured_deployment(
            &bundle,
            Some(&result.id),
            &manager,
            &admission,
            &handles,
            |_, _, _| panic!(),
            |_| panic!(),
            &mut repeated_effects
        )
        .await
        .unwrap(),
        DeploymentLookup::Complete { .. }
    ));
    assert!(!repeated_effects);
    fs::write(&config, &empty).unwrap();
    let delete = capture_deployment(dir.path()).unwrap();
    assert!(
        matches!(apply_captured_deployment(&delete, None, &manager, &admission, &handles, |_, _, _| {}, |_| {}, &mut effects).await.unwrap(), DeploymentLookup::Complete { result } if result.converged)
    );
    assert!(!target.exists());
    assert_eq!(
        prepare_deployment_preview(&bundle, &admission, &manager)
            .await
            .unwrap_err()
            .code,
        "restore_destination_used"
    );
    for (root, ledger, sequence, graph) in [
        ("file:///other", receipt.ledger_id.as_str(), 2, "knowledge"),
        (admission.canonical_root(), "other-ledger", 2, "knowledge"),
        (
            admission.canonical_root(),
            receipt.ledger_id.as_str(),
            3,
            "knowledge",
        ),
        (
            admission.canonical_root(),
            receipt.ledger_id.as_str(),
            2,
            "other",
        ),
    ] {
        assert!(
            receipt
                .validate_binding(root, ledger, sequence, graph)
                .is_err()
        );
    }
}
