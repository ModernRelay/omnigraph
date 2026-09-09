//! An identity-authorized schema change cannot inherit an older data write's
//! recovery effects. Fault injection lives in a separate integration process.

#![cfg(feature = "failpoints")]

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use omnigraph::db::Omnigraph;
use omnigraph::failpoints::{FailScenario, ScopedFailPoint, names};
use omnigraph_cluster::{
    ApplyOptions, IdentityAuthorization, PlanOptions, apply_config_dir,
    apply_config_dir_authorized, authorize_apply_plan, import_config_dir,
    plan_config_dir_authorized,
};

const SCHEMA: &str = "node Person { name: String @key }";

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
    let imported = import_config_dir(dir.path()).await;
    assert!(imported.ok, "{:?}", imported.diagnostics);
    let applied = apply_config_dir(dir.path()).await;
    assert!(applied.ok && applied.converged, "{:?}", applied.diagnostics);

    fs::write(
        dir.path().join("people.pg"),
        "node Person { name: String @key\n email: String? }",
    )
    .unwrap();
    let caller = IdentityAuthorization::authenticated("principal:schema").unwrap();
    let planned =
        plan_config_dir_authorized(dir.path(), PlanOptions { observe: true }, &caller).await;
    assert!(planned.plan.ok, "{:?}", planned.plan.diagnostics);
    let expected = planned.authorization.unwrap();

    // This is a real effects-confirmed Mutation sidecar, not hand-written JSON.
    // The table transaction has committed while the graph manifest is unchanged.
    let graph = dir.path().join("graphs/knowledge.omni");
    let uri = graph.to_str().unwrap();
    let writer = Omnigraph::open(uri).await.unwrap();
    {
        let _failpoint =
            ScopedFailPoint::new(names::MUTATION_POST_FINALIZE_PRE_PUBLISHER, "return");
        let error = writer
            .mutate_as(
                "main",
                "query add() { insert Person { name: \"interrupted\" } }",
                "add",
                &Default::default(),
                Some("principal:writer"),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("injected failpoint"), "{error}");
    }
    drop(writer);
    assert_eq!(fs::read_dir(graph.join("__recovery")).unwrap().count(), 1);
    let before_graph = file_bytes(&graph);
    let ledger = dir.path().join("__cluster/state.json");
    let before_ledger = fs::read(&ledger).unwrap();

    let preview =
        plan_config_dir_authorized(dir.path(), PlanOptions { observe: true }, &caller).await;
    assert!(
        !preview.plan.ok,
        "pending data recovery must block a new plan"
    );
    assert!(preview.authorization.is_none());
    assert!(
        authorize_apply_plan(dir.path(), &caller, &expected)
            .await
            .is_err()
    );
    let refused =
        apply_config_dir_authorized(dir.path(), ApplyOptions::default(), &caller, &expected).await;
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

    // The existing explicit storage-holder path keeps its recovery behavior.
    let legacy = apply_config_dir(dir.path()).await;
    assert!(legacy.ok && legacy.converged, "{:?}", legacy.diagnostics);
    assert_eq!(fs::read_dir(graph.join("__recovery")).unwrap().count(), 0);
    let recovered = Omnigraph::open_read_only(uri).await.unwrap();
    assert!(recovered.schema_source().contains("email"));
    let result = recovered
        .query(
            "main",
            "query names() { match { $p: Person } return { $p.name } }",
            "names",
            &Default::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        result.num_rows(),
        1,
        "original data write recovers through Tier 0"
    );
}
