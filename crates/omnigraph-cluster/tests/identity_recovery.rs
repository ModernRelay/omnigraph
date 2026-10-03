//! An identity-authorized schema change cannot inherit an older data write's
//! recovery effects. Fault injection lives in a separate integration process.

#![cfg(feature = "failpoints")]

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use omnigraph::db::{Omnigraph, ReadTarget, SchemaApplySettlement, SchemaNonPublicationProof};
use omnigraph::seams::catalog;
use omnigraph_cluster::seams::{FailScenario, catalog as cluster_seams};
use omnigraph_cluster::{
    ApplyOptions, AuthorityKind, DeploymentCaller, DeploymentLookup, GraphDeploymentResult,
    IdentityAuthorization, PlanOptions, apply_config_dir, apply_config_dir_authorized,
    apply_deployment, authorize_apply_plan, deployment_status, force_unlock_storage_root,
    import_config_dir, plan_config_dir_authorized, reconcile_deployment, upgrade_deployment_ledger,
};

const SCHEMA: &str = "node Person { name: String @key }";

fn session(db: Omnigraph) -> omnigraph::Session {
    omnigraph::Session::from_defaults(
        std::sync::Arc::new(db),
        omnigraph::settings::SessionSettings::default(),
    )
}

fn file_bytes(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn collect(base: &Path, path: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                collect(base, &path, files);
            } else {
                files.insert(
                    path.strip_prefix(base).unwrap().to_owned(),
                    fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut files = BTreeMap::new();
    collect(root, root, &mut files);
    files
}

#[tokio::test]
#[serial_test::serial]
async fn identity_schema_apply_refuses_real_pending_data_recovery_without_effects() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("people.pg"), SCHEMA).unwrap();
    fs::write(
        dir.path().join("graph.policy.yaml"),
        "version: 1\ngroups:\n  schema: [principal:schema]\nrules:\n  - id: schema-read\n    allow: { actors: { group: schema }, actions: [read] }\n  - id: schema-apply\n    allow: { actors: { group: schema }, actions: [schema_apply], target_branch_scope: any }\n",
    )
    .unwrap();
    fs::write(
        dir.path().join("cluster.policy.yaml"),
        "version: 1\ngroups:\n  owners: [principal:operator]\nrules:\n  - id: config\n    allow: { actors: { group: owners }, actions: [config_manage] }\n",
    )
    .unwrap();
    fs::write(
        dir.path().join("cluster.yaml"),
        "version: 1\ngraphs:\n  knowledge:\n    schema: ./people.pg\npolicies:\n  graph:\n    file: ./graph.policy.yaml\n    applies_to: [knowledge]\n  management:\n    file: ./cluster.policy.yaml\n    applies_to: [cluster]\n",
    )
    .unwrap();
    let imported = Box::pin(import_config_dir(dir.path())).await;
    assert!(imported.ok, "{:?}", imported.diagnostics);
    let applied = Box::pin(apply_config_dir(dir.path())).await;
    assert!(applied.ok && applied.converged, "{:?}", applied.diagnostics);

    fs::write(
        dir.path().join("people.pg"),
        "node Person { name: String @key\n email: String? }",
    )
    .unwrap();
    let caller = IdentityAuthorization::authenticated("principal:schema").unwrap();
    let planned = Box::pin(plan_config_dir_authorized(
        dir.path(),
        PlanOptions { observe: true },
        &caller,
    ))
    .await;
    assert!(planned.plan.ok, "{:?}", planned.plan.diagnostics);
    let expected = planned.authorization.unwrap();

    // RFC 0067: an interrupted mutation arms no recovery — its detached
    // staging is unreachable garbage — so it must NOT block planning. Only a
    // sidecar from a build that predates detached commits can occupy
    // `__recovery/`, and this build cannot interpret one; plant that.
    let graph = dir.path().join("graphs/knowledge.omni");
    let uri = graph.to_str().unwrap();
    let writer = session(Box::pin(Omnigraph::open(uri)).await.unwrap());
    {
        let _failpoint = catalog::MUTATION_POST_FINALIZE_PRE_PUBLISHER.fire_always();
        let error = Box::pin(writer.mutate_as(
            "main",
            "query add() { insert Person { name: \"interrupted\" } }",
            "add",
            &Default::default(),
            Some("principal:writer"),
        ))
        .await
        .unwrap_err();
        assert!(error.to_string().contains("injected failpoint"), "{error}");
    }
    drop(writer);
    assert!(
        !graph.join("__recovery").exists(),
        "an interrupted mutation leaves no recovery record (RFC 0067)"
    );
    let unblocked = Box::pin(plan_config_dir_authorized(
        dir.path(),
        PlanOptions { observe: true },
        &caller,
    ))
    .await;
    assert!(
        unblocked.plan.ok,
        "an interrupted mutation must not block planning: {:?}",
        unblocked.plan.diagnostics
    );
    fs::create_dir_all(graph.join("__recovery")).unwrap();
    fs::write(graph.join("__recovery/01LEGACYSIDECAR.json"), "{}").unwrap();
    let before_graph = file_bytes(&graph);
    let ledger = dir.path().join("__cluster/state.json");
    let before_ledger = fs::read(&ledger).unwrap();

    let preview = Box::pin(plan_config_dir_authorized(
        dir.path(),
        PlanOptions { observe: true },
        &caller,
    ))
    .await;
    assert!(
        !preview.plan.ok,
        "pending data recovery must block a new plan"
    );
    assert!(preview.authorization.is_none());
    assert!(
        Box::pin(authorize_apply_plan(dir.path(), &caller, &expected))
            .await
            .is_err()
    );
    let refused = Box::pin(apply_config_dir_authorized(
        dir.path(),
        ApplyOptions::default(),
        &caller,
        &expected,
    ))
    .await;
    assert!(
        !refused.apply.ok,
        "pending data recovery must block schema apply"
    );
    assert!(
        refused.authorization.is_none(),
        "refusal must precede effects"
    );
    assert!(
        file_bytes(&graph) == before_graph,
        "no graph recovery or schema writes"
    );
    assert_eq!(
        fs::read(&ledger).unwrap(),
        before_ledger,
        "no ledger writes"
    );

    // The explicit storage-holder path refuses the sidecar too: this build
    // cannot interpret one, and only the build that wrote it may resolve it.
    let refused_legacy = Box::pin(apply_config_dir(dir.path())).await;
    assert!(
        !refused_legacy.ok,
        "the storage-holder path must refuse a legacy sidecar: {:?}",
        refused_legacy.diagnostics
    );
    fs::remove_file(graph.join("__recovery/01LEGACYSIDECAR.json")).unwrap();
    let resolved = Box::pin(apply_config_dir(dir.path())).await;
    assert!(
        resolved.ok && resolved.converged,
        "{:?}",
        resolved.diagnostics
    );
    assert_eq!(fs::read_dir(graph.join("__recovery")).unwrap().count(), 0);
    let recovered = session(Box::pin(Omnigraph::open_read_only(uri)).await.unwrap());
    assert!(recovered.schema_source().contains("email"));
    let result = Box::pin(recovered.query(
        "main",
        "query names() { match { $p: Person } return { $p.name } }",
        "names",
        &Default::default(),
    ))
    .await
    .unwrap();
    assert_eq!(
        result.num_rows(),
        0,
        "an unacknowledged write is never resurrected (RFC 0067)"
    );
}

#[tokio::test]
#[serial_test::serial]
async fn offline_recovery_reauthorizes_executor_and_preserves_original_and_fence_authors() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    fs::write(dir.path().join("people.pg"), SCHEMA).unwrap();
    let actors = "[principal:original, principal:recovery, principal:adopter]";
    fs::write(
        dir.path().join("graph.policy.yaml"),
        format!("version: 1\ngroups:\n  operators: {actors}\nrules:\n  - id: read\n    allow: {{ actors: {{ group: operators }}, actions: [read] }}\n  - id: schema\n    allow: {{ actors: {{ group: operators }}, actions: [schema_apply], target_branch_scope: any }}\n"),
    ).unwrap();
    fs::write(
        dir.path().join("cluster.policy.yaml"),
        format!("version: 1\ngroups:\n  operators: {actors}\nrules:\n  - id: config\n    allow: {{ actors: {{ group: operators }}, actions: [config_manage] }}\n"),
    ).unwrap();
    fs::write(
        dir.path().join("cluster.yaml"),
        "version: 1\ngraphs:\n  knowledge:\n    schema: people.pg\npolicies:\n  graph:\n    file: graph.policy.yaml\n    applies_to: [knowledge]\n  management:\n    file: cluster.policy.yaml\n    applies_to: [cluster]\n",
    ).unwrap();
    let imported = Box::pin(import_config_dir(dir.path())).await;
    assert!(imported.ok, "{imported:?}");
    let applied = Box::pin(apply_config_dir(dir.path())).await;
    assert!(applied.ok && applied.converged, "{applied:?}");
    let graph = dir.path().join("graphs/knowledge.omni");
    let uri = graph.to_str().unwrap();
    let writer = session(Box::pin(Omnigraph::open(uri)).await.unwrap());
    Box::pin(writer.mutate(
        "main",
        "query seed() { insert Person { name: \"Ada\" } }",
        "seed",
        &Default::default(),
    ))
    .await
    .unwrap();
    let rows = writer.export_jsonl("main", &[]).await.unwrap();
    let version = writer
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .graph_manifest_version();
    let contract = writer.schema_contract_digest();
    let history_len = writer.list_commits(None).await.unwrap().len();
    drop(writer);
    let identity = |actor| {
        DeploymentCaller::AuthenticatedIdentity(
            IdentityAuthorization::authenticated(actor).unwrap(),
        )
    };
    let original = identity("principal:original");
    let recovery = identity("principal:recovery");
    let adopter = identity("principal:adopter");
    let before_denied_conversion = file_bytes(dir.path());
    let denied = Box::pin(upgrade_deployment_ledger(
        root,
        true,
        &identity("principal:unpermitted"),
    ))
    .await
    .unwrap_err();
    assert_eq!(denied.code, "policy_denied");
    assert!(
        file_bytes(dir.path()) == before_denied_conversion,
        "denied conversion must not change graph, ledger, catalog or admission bytes"
    );
    Box::pin(upgrade_deployment_ledger(root, true, &original))
        .await
        .unwrap();
    let ledger_path = dir.path().join("__cluster/state.json");
    let original_state: serde_json::Value =
        serde_json::from_slice(&fs::read(&ledger_path).unwrap()).unwrap();
    fs::write(
        dir.path().join("people.pg"),
        "node Person { name: String @key\n email: String? }",
    )
    .unwrap();
    let graph_bytes = file_bytes(&graph);
    let mut id = String::new();
    {
        let _fail = cluster_seams::DEPLOYMENT_AFTER_STARTED.fire_always();
        let error = Box::pin(apply_deployment(
            dir.path(),
            None,
            &original,
            |issued, _, _| id = issued.to_string(),
        ))
        .await
        .unwrap_err();
        assert_eq!(error.code, "injected_failpoint");
    }
    let started: serde_json::Value =
        serde_json::from_slice(&fs::read(&ledger_path).unwrap()).unwrap();
    let original_commit =
        started["outstanding"]["graphs"]["knowledge"]["intent"]["lineage"]["graph_commit_id"]
            .as_str()
            .unwrap()
            .to_string();
    assert_eq!(
        started["outstanding"]["graphs"]["knowledge"]["state"]["state"],
        "started"
    );
    let lock = deployment_status(root, Some(&id), &original)
        .await
        .unwrap()
        .lock_id
        .unwrap();
    force_unlock_storage_root(root, &lock).await.unwrap();

    // Neither an original intent nor an existing deployment ID grants access.
    let ledger_before_denial = fs::read(&ledger_path).unwrap();
    let denied = Box::pin(reconcile_deployment(
        root,
        &id,
        true,
        &identity("principal:unpermitted"),
    ))
    .await
    .unwrap_err();
    assert_eq!(denied.code, "policy_denied");
    assert!(!dir.path().join("__cluster/lock.json").exists());
    assert_eq!(fs::read(&ledger_path).unwrap(), ledger_before_denial);
    assert_eq!(file_bytes(&graph), graph_bytes);

    {
        let _fail = cluster_seams::DEPLOYMENT_AFTER_SETTLEMENT_INTENT.fire_always();
        let error = Box::pin(reconcile_deployment(root, &id, true, &recovery))
            .await
            .unwrap_err();
        assert_eq!(error.code, "injected_failpoint");
    }
    let prepared: serde_json::Value =
        serde_json::from_slice(&fs::read(&ledger_path).unwrap()).unwrap();
    let fence = prepared["outstanding"]["graphs"]["knowledge"]["settlement"].clone();
    assert_eq!(fence["lineage"]["actor_id"], "principal:recovery");
    assert_eq!(
        prepared["outstanding"]["graphs"]["knowledge"]["recovery_executor"]["actor"],
        "principal:recovery"
    );
    assert_eq!(
        prepared["outstanding"]["authorization"]["authority"]["actor"],
        "principal:original"
    );
    assert_eq!(
        file_bytes(&graph),
        graph_bytes,
        "persisting a fence identity executes no graph effect"
    );
    let lock = deployment_status(root, Some(&id), &recovery)
        .await
        .unwrap()
        .lock_id
        .unwrap();
    force_unlock_storage_root(root, &lock).await.unwrap();
    fs::remove_file(dir.path().join("people.pg")).unwrap();
    fs::remove_file(dir.path().join("cluster.yaml")).unwrap();

    // The next permitted actor must adopt exactly the durable fence identity.
    let DeploymentLookup::Complete { result } =
        Box::pin(reconcile_deployment(root, &id, true, &adopter))
            .await
            .unwrap()
    else {
        panic!("expected terminal original deployment result");
    };
    assert!(!result.converged);
    assert_eq!(result.id, id);
    assert_eq!(result.authority.kind, AuthorityKind::AuthenticatedIdentity);
    assert_eq!(
        result.authority.actor.as_deref(),
        Some("principal:original")
    );
    assert_eq!(
        result.recovery_executors["knowledge"].kind,
        AuthorityKind::AuthenticatedIdentity
    );
    assert_eq!(
        result.recovery_executors["knowledge"].actor.as_deref(),
        Some("principal:adopter")
    );
    let GraphDeploymentResult::Schema {
        result:
            SchemaApplySettlement::NotPublished {
                proof:
                    SchemaNonPublicationProof::Fence {
                        commit,
                        contract: fenced_contract,
                    },
            },
    } = &result.graphs["knowledge"]
    else {
        panic!("expected neutral-fence proof: {:?}", result.graphs);
    };
    assert_eq!(
        commit.graph_commit_id,
        fence["lineage"]["graph_commit_id"].as_str().unwrap()
    );
    assert_ne!(commit.graph_commit_id, original_commit);
    assert_eq!(commit.actor_id.as_deref(), Some("principal:recovery"));
    assert_eq!(*fenced_contract, contract);
    let db = Box::pin(Omnigraph::open_read_only(uri)).await.unwrap();
    assert_eq!(db.schema_source().as_str(), SCHEMA);
    assert_eq!(db.schema_contract_digest(), contract);
    assert_eq!(db.export_jsonl("main", &[]).await.unwrap(), rows);
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .graph_manifest_version(),
        version + 1
    );
    let history = db.list_commits(None).await.unwrap();
    assert_eq!(history.len(), history_len + 1);
    assert!(
        history
            .iter()
            .all(|entry| entry.graph_commit_id != original_commit)
    );
    let finished: serde_json::Value =
        serde_json::from_slice(&fs::read(&ledger_path).unwrap()).unwrap();
    assert_eq!(
        finished["applied_revision"]["resources"],
        original_state["applied_revision"]["resources"]
    );
    assert_eq!(
        finished["applied_revision"]["schema_contracts"],
        original_state["applied_revision"]["schema_contracts"]
    );
    assert_eq!(
        finished["applied_revision"]["result_revision"],
        original_state["applied_revision"]["result_revision"]
    );
    assert!(finished.get("outstanding").is_none());
    assert!(
        deployment_status(root, Some(&id), &adopter)
            .await
            .unwrap()
            .lock_id
            .is_some()
    );
}
