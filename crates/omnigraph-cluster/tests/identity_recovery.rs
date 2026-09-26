//! An identity-authorized schema change cannot inherit an older data write's
//! recovery effects. Fault injection lives in a separate integration process.

#![cfg(feature = "failpoints")]

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use omnigraph::db::Omnigraph;
use omnigraph::seams::{FailScenario, catalog};
use omnigraph_cluster::{
    ApplyOptions, IdentityAuthorization, PlanOptions, apply_config_dir,
    apply_config_dir_authorized, authorize_apply_plan, import_config_dir,
    plan_config_dir_authorized,
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
