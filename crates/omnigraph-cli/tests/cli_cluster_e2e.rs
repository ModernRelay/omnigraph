//! Cluster lifecycle compositions over the spawned binary (recovery, drift, convergence).
//! Moved verbatim from tests/cli.rs in the modularization.

use std::fs;

use tempfile::tempdir;

mod support;

use support::*;


#[test]
fn cluster_e2e_lifecycle_import_apply_status_refresh_converges() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    init_cluster_derived_graph(temp.path());

    let import = cluster_json(temp.path(), "import");
    assert_eq!(import["ok"], true, "{import}");
    assert_eq!(import["state_observations"]["state_revision"], 1);

    let plan = cluster_json(temp.path(), "plan");
    let changes = plan["changes"].as_array().unwrap();
    assert_eq!(changes.len(), 3, "{plan}");
    let disposition_of = |resource: &str| {
        changes
            .iter()
            .find(|change| change["resource"] == resource)
            .unwrap_or_else(|| panic!("missing change for {resource}: {plan}"))["disposition"]
            .clone()
    };
    assert_eq!(disposition_of("graph.knowledge"), "derived");
    assert_eq!(disposition_of("query.knowledge.find_person"), "applied");
    assert_eq!(disposition_of("policy.base"), "applied");

    let apply = cluster_json(temp.path(), "apply");
    assert_eq!(apply["ok"], true, "{apply}");
    assert_eq!(apply["applied_count"], 2, "{apply}");
    assert_eq!(apply["converged"], true, "{apply}");

    let status = cluster_json(temp.path(), "status");
    assert_eq!(
        status["resource_statuses"]["query.knowledge.find_person"]["status"],
        "applied"
    );
    assert_eq!(status["resource_statuses"]["policy.base"]["status"], "applied");
    assert!(
        status["state_observations"]["applied_config_digest"].is_string(),
        "converged apply must record the applied config digest: {status}"
    );

    // Refresh re-observes the live graph; it must not undo apply's work.
    let refresh = cluster_json(temp.path(), "refresh");
    assert_eq!(refresh["ok"], true, "{refresh}");
    let replan = cluster_json(temp.path(), "plan");
    assert!(
        replan["changes"].as_array().unwrap().is_empty(),
        "refresh after a converged apply must not re-open the plan: {replan}"
    );

    // A query edit round-trips: plan update -> apply -> converged again.
    fs::write(
        temp.path().join("people.gq"),
        r#"
query find_person($name: String) {
  match { $p: Person { name: $name } }
  return { $p.name }
}
"#,
    )
    .unwrap();
    let apply_edit = cluster_json(temp.path(), "apply");
    assert_eq!(apply_edit["applied_count"], 1, "{apply_edit}");
    assert_eq!(apply_edit["converged"], true, "{apply_edit}");

    let final_apply = cluster_json(temp.path(), "apply");
    assert_eq!(final_apply["state_written"], false, "{final_apply}");
    assert!(final_apply["changes"].as_array().unwrap().is_empty());
}

#[test]
fn cluster_e2e_schema_change_applied_by_cluster() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    init_cluster_derived_graph(temp.path());
    let import = cluster_json(temp.path(), "import");
    assert_eq!(import["ok"], true, "{import}");
    let apply = cluster_json(temp.path(), "apply");
    assert_eq!(apply["converged"], true, "{apply}");

    // Additive schema change: Stage 4B applies it from the cluster — no
    // manual schema apply, no refresh round-trip.
    fs::write(
        temp.path().join("people.pg"),
        r#"
node Person {
  name: String @key
  age: I32?
  bio: String?
}
"#,
    )
    .unwrap();

    // Plan previews the real migration steps (RFC-004 §D7).
    let plan = cluster_json(temp.path(), "plan");
    let schema_change = change_for(&plan, "schema.knowledge");
    assert_eq!(schema_change["disposition"], "applied", "{plan}");
    let migration = &schema_change["migration"];
    assert_eq!(migration["supported"], true, "{plan}");
    assert!(
        migration["steps"]
            .as_array()
            .unwrap()
            .iter()
            .any(|step| step["kind"] == "add_property"),
        "{plan}"
    );

    let evolve = cluster_json(temp.path(), "apply");
    assert_eq!(evolve["ok"], true, "{evolve}");
    assert_eq!(evolve["converged"], true, "{evolve}");
    assert_eq!(change_for(&evolve, "schema.knowledge")["disposition"], "applied");

    // The live graph carries the new schema; the plan is empty.
    let schema_show = output_success(
        cli()
            .arg("schema")
            .arg("show")
            .arg(temp.path().join("graphs/knowledge.omni")),
    );
    assert!(stdout_string(&schema_show).contains("bio"), "live schema updated");
    let replan = cluster_json(temp.path(), "plan");
    assert!(
        replan["changes"].as_array().unwrap().is_empty(),
        "one cluster apply converges a schema change: {replan}"
    );
}

#[test]
fn cluster_e2e_force_unlock_unblocks_apply() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    write_cluster_applyable_state(temp.path());
    write_cluster_lock(temp.path(), "stuck-lock", "apply");

    let refused = parse_stdout_json(&output_failure(
        cli()
            .arg("cluster")
            .arg("apply")
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(refused["ok"], false);

    let unlocked = parse_stdout_json(&output_success(
        cli()
            .arg("cluster")
            .arg("force-unlock")
            .arg("stuck-lock")
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(unlocked["lock_removed"], true, "{unlocked}");

    let retried = cluster_json(temp.path(), "apply");
    assert_eq!(retried["ok"], true, "{retried}");
    assert_eq!(retried["converged"], true, "{retried}");
}

#[test]
fn cluster_e2e_lost_state_reimport_recovers_catalog() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    init_cluster_derived_graph(temp.path());
    let import = cluster_json(temp.path(), "import");
    assert_eq!(import["ok"], true, "{import}");
    let apply = cluster_json(temp.path(), "apply");
    assert_eq!(apply["converged"], true, "{apply}");

    let query_digest = change_for(&apply, "query.knowledge.find_person")["after_digest"]
        .as_str()
        .unwrap()
        .to_string();
    let blob = temp
        .path()
        .join("__cluster/resources/query/knowledge/find_person")
        .join(format!("{query_digest}.gq"));
    let blob_content = fs::read_to_string(&blob).unwrap();

    // Disaster: the state ledger is lost.
    fs::remove_file(temp.path().join("__cluster/state.json")).unwrap();

    let reimport = cluster_json(temp.path(), "import");
    assert_eq!(reimport["ok"], true, "{reimport}");
    assert_eq!(reimport["state_observations"]["state_revision"], 1);
    // Import observes graph/schema only; query/policy digests are not invented.
    assert!(
        reimport["resource_digests"]
            .get("query.knowledge.find_person")
            .is_none(),
        "{reimport}"
    );

    let plan = cluster_json(temp.path(), "plan");
    assert_eq!(
        change_for(&plan, "query.knowledge.find_person")["disposition"],
        "applied"
    );
    assert_eq!(change_for(&plan, "policy.base")["disposition"], "applied");

    let reapply = cluster_json(temp.path(), "apply");
    assert_eq!(reapply["ok"], true, "{reapply}");
    assert_eq!(reapply["converged"], true, "{reapply}");
    assert!(
        reapply["state_observations"]["applied_config_digest"].is_string(),
        "{reapply}"
    );
    // The catalog blob was reused, not rewritten with different content.
    assert_eq!(fs::read_to_string(&blob).unwrap(), blob_content);

    let replan = cluster_json(temp.path(), "plan");
    assert!(replan["changes"].as_array().unwrap().is_empty(), "{replan}");
}

#[test]
fn cluster_e2e_out_of_band_schema_drift_then_apply_converges_it() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    init_cluster_derived_graph(temp.path());
    let import = cluster_json(temp.path(), "import");
    assert_eq!(import["ok"], true, "{import}");
    let apply = cluster_json(temp.path(), "apply");
    assert_eq!(apply["converged"], true, "{apply}");

    // Out-of-band: the live graph evolves, cluster.yaml stays put.
    fs::write(
        temp.path().join("people_v2.pg"),
        r#"
node Person {
  name: String @key
  age: I32?
  bio: String?
}
"#,
    )
    .unwrap();
    output_success(
        cli()
            .arg("schema")
            .arg("apply")
            .arg(temp.path().join("graphs/knowledge.omni"))
            .arg("--schema")
            .arg(temp.path().join("people_v2.pg"))
            .arg("--json"),
    );

    // Drift is visible...
    let refresh = cluster_json(temp.path(), "refresh");
    assert_eq!(
        refresh["resource_statuses"]["schema.knowledge"]["status"],
        "drifted"
    );
    // ...the plan proposes converging back to desired, with a migration
    // preview (a soft drop of the out-of-band field)...
    let plan = cluster_json(temp.path(), "plan");
    let schema_change = change_for(&plan, "schema.knowledge");
    assert_eq!(schema_change["disposition"], "applied", "{plan}");
    assert!(
        schema_change["migration"]["steps"]
            .as_array()
            .unwrap()
            .iter()
            .any(|step| step["kind"] == "drop_property" && step["mode"] == "soft"),
        "{plan}"
    );
    // ...and apply converges the live schema back (axiom 8: drift correction
    // is gated like any change; a soft migration is the recoverable tier).
    let converge = cluster_json(temp.path(), "apply");
    assert_eq!(converge["ok"], true, "{converge}");
    assert_eq!(converge["converged"], true, "{converge}");
    let schema_show = output_success(
        cli()
            .arg("schema")
            .arg("show")
            .arg(temp.path().join("graphs/knowledge.omni")),
    );
    assert!(
        !stdout_string(&schema_show).contains("bio"),
        "out-of-band field soft-dropped back to desired"
    );
    let replan = cluster_json(temp.path(), "plan");
    assert!(replan["changes"].as_array().unwrap().is_empty(), "{replan}");
}

#[test]
fn cluster_e2e_graph_root_destruction_drifts_then_apply_recreates_empty_graph() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    init_cluster_derived_graph(temp.path());
    let import = cluster_json(temp.path(), "import");
    assert_eq!(import["ok"], true, "{import}");
    let apply = cluster_json(temp.path(), "apply");
    assert_eq!(apply["converged"], true, "{apply}");
    let query_digest = change_for(&apply, "query.knowledge.find_person")["after_digest"]
        .as_str()
        .unwrap()
        .to_string();

    fs::remove_dir_all(temp.path().join("graphs/knowledge.omni")).unwrap();

    // Missing root is drift, not an error.
    let refresh = cluster_json(temp.path(), "refresh");
    assert_eq!(refresh["ok"], true, "{refresh}");
    assert_eq!(
        refresh["resource_statuses"]["graph.knowledge"]["status"],
        "drifted"
    );
    assert!(
        refresh["resource_statuses"]["graph.knowledge"]["conditions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|condition| condition == "graph_missing"),
        "{refresh}"
    );
    // Graph/schema digests removed; query/policy digests preserved.
    assert!(refresh["resource_digests"].get("graph.knowledge").is_none());
    assert!(refresh["resource_digests"].get("schema.knowledge").is_none());
    assert!(
        refresh["resource_digests"]
            .get("query.knowledge.find_person")
            .is_some(),
        "{refresh}"
    );

    let plan = cluster_json(temp.path(), "plan");
    assert_eq!(change_for(&plan, "graph.knowledge")["operation"], "create");
    // Stage 4A: the re-create is executable and the plan says so — nothing
    // hidden about converging a destroyed root back to an EMPTY graph (the
    // data was already lost; this is declarative convergence, RFC-004 §D1).
    assert_eq!(change_for(&plan, "graph.knowledge")["disposition"], "applied");
    assert_eq!(change_for(&plan, "schema.knowledge")["disposition"], "applied");
    // Converged-then-destroyed: query/policy are already in state at the
    // desired digests, so they are not changes at all.
    assert_eq!(plan["changes"].as_array().unwrap().len(), 2, "{plan}");

    let recreate = cluster_json(temp.path(), "apply");
    assert_eq!(recreate["ok"], true, "{recreate}");
    assert_eq!(recreate["converged"], true, "{recreate}");
    // The empty graph is back on disk; catalog state survived throughout.
    assert!(temp.path().join("graphs/knowledge.omni").exists());
    let state: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(temp.path().join("__cluster/state.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        state["applied_revision"]["resources"]["query.knowledge.find_person"]["digest"],
        query_digest
    );
    assert!(
        temp.path()
            .join("__cluster/resources/query/knowledge/find_person")
            .join(format!("{query_digest}.gq"))
            .exists()
    );
}

#[test]
fn cluster_e2e_multi_graph_mixed_dispositions_then_approve_and_converge() {
    let temp = tempdir().unwrap();
    write_multi_graph_cluster_fixture(temp.path());
    // No manual init: Stage 4A creates both graphs.

    let import = cluster_json(temp.path(), "import");
    assert_eq!(import["ok"], true, "{import}");

    let apply = cluster_json(temp.path(), "apply");
    assert_eq!(apply["ok"], true, "{apply}");
    assert_eq!(apply["converged"], true, "{apply}");
    assert_eq!(change_for(&apply, "graph.knowledge")["disposition"], "applied");
    assert_eq!(
        change_for(&apply, "graph.engineering")["disposition"],
        "applied"
    );
    assert_eq!(
        change_for(&apply, "query.engineering.find_service")["disposition"],
        "applied"
    );
    // The graph-spanning and cluster-scoped policies ride the same run.
    assert_eq!(change_for(&apply, "policy.shared")["disposition"], "applied");
    assert_eq!(
        change_for(&apply, "policy.cluster_wide")["disposition"],
        "applied"
    );
    assert!(temp.path().join("graphs/knowledge.omni").exists());
    assert!(temp.path().join("graphs/engineering.omni").exists());

    // Mixed run: a graph REMOVAL (4C territory — deferred) gates its query
    // delete (blocked), while a knowledge query update is independent
    // (applied) and re-derives its composite. All four dispositions at once.
    fs::write(
        temp.path().join("cluster.yaml"),
        r#"
version: 1
metadata:
  name: company-brain
state:
  backend: cluster
  lock: true
graphs:
  knowledge:
    schema: ./people.pg
    queries:
      find_person:
        file: ./people.gq
policies:
  shared:
    file: ./shared.policy.yaml
    applies_to: [knowledge]
  cluster_wide:
    file: ./cluster_wide.policy.yaml
    applies_to: [cluster]
"#,
    )
    .unwrap();
    fs::write(
        temp.path().join("people.gq"),
        "\nquery find_person($name: String) {\n  match { $p: Person { name: $name } }\n  return { $p.name }\n}\n",
    )
    .unwrap();

    let mixed = cluster_json(temp.path(), "apply");
    assert_eq!(mixed["ok"], true, "{mixed}");
    assert_eq!(mixed["converged"], false, "{mixed}");
    // Stage 4C: deletes are gated on a digest-bound approval, one gate per
    // subtree (the graph-level approval carries schema + queries).
    assert_eq!(
        change_for(&mixed, "graph.engineering")["disposition"],
        "blocked"
    );
    assert_eq!(
        change_for(&mixed, "graph.engineering")["reason"],
        "approval_required"
    );
    assert_eq!(
        change_for(&mixed, "schema.engineering")["reason"],
        "approval_required"
    );
    assert_eq!(
        change_for(&mixed, "query.engineering.find_service")["reason"],
        "approval_required"
    );
    let gate_plan = cluster_json(temp.path(), "plan");
    let gates = gate_plan["approvals_required"].as_array().unwrap();
    assert_eq!(gates.len(), 1, "{gate_plan}");
    assert_eq!(gates[0]["resource"], "graph.engineering");
    assert_eq!(gates[0]["satisfied"], false);
    assert_eq!(
        change_for(&mixed, "query.knowledge.find_person")["disposition"],
        "applied"
    );
    // 5A: policy.shared's applies_to narrowed with an unchanged file digest
    // — now a first-class binding change, applied in the same run.
    assert_eq!(change_for(&mixed, "policy.shared")["binding_change"], true);
    assert_eq!(change_for(&mixed, "policy.shared")["disposition"], "applied");
    assert_eq!(
        change_for(&mixed, "graph.knowledge")["disposition"],
        "derived"
    );
    // Deterministic ordering: changes sorted by resource address.
    let order: Vec<&str> = mixed["changes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|change| change["resource"].as_str().unwrap())
        .collect();
    let mut sorted = order.clone();
    sorted.sort_unstable();
    assert_eq!(order, sorted, "{mixed}");
    // The conclusion: an apply without approval stays blocked; the approved
    // delete converges the cluster, tombstoning the removed graph.
    let still_blocked = cluster_json(temp.path(), "apply");
    assert_eq!(still_blocked["converged"], false, "{still_blocked}");

    let approve = parse_stdout_json(&output_success(
        cli()
            .arg("--as")
            .arg("andrew")
            .arg("cluster")
            .arg("approve")
            .arg("graph.engineering")
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(approve["ok"], true, "{approve}");
    assert_eq!(approve["approved_by"], "andrew");

    let converge = cluster_json(temp.path(), "apply");
    assert_eq!(converge["ok"], true, "{converge}");
    assert_eq!(converge["converged"], true, "{converge}");
    assert!(!temp.path().join("graphs/engineering.omni").exists());

    let status = cluster_json(temp.path(), "status");
    assert_eq!(status["observations"]["graph.engineering"]["kind"], "tombstone");
    let final_plan = cluster_json(temp.path(), "plan");
    assert!(
        final_plan["changes"].as_array().unwrap().is_empty(),
        "{final_plan}"
    );
}

#[test]
fn cluster_e2e_approve_requires_actor() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());

    let output = output_failure(
        cli()
            .arg("cluster")
            .arg("approve")
            .arg("graph.knowledge")
            .arg("--config")
            .arg(temp.path()),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--as"), "{stderr}");
}

#[test]
fn cluster_e2e_declared_graph_created_by_apply() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());

    let import = cluster_json(temp.path(), "import");
    assert_eq!(import["ok"], true, "{import}");

    let apply = cluster_json(temp.path(), "apply");
    assert_eq!(apply["ok"], true, "{apply}");
    assert_eq!(apply["converged"], true, "{apply}");
    assert_eq!(change_for(&apply, "graph.knowledge")["disposition"], "applied");
    assert!(temp.path().join("graphs/knowledge.omni").exists());

    // The created graph is a real graph: the per-graph CLI can open it.
    let snapshot = output_success(
        cli()
            .arg("snapshot")
            .arg(temp.path().join("graphs/knowledge.omni")),
    );
    assert!(!stdout_string(&snapshot).is_empty());

    let plan = cluster_json(temp.path(), "plan");
    assert!(plan["changes"].as_array().unwrap().is_empty(), "{plan}");
    let status = cluster_json(temp.path(), "status");
    assert_eq!(
        status["resource_statuses"]["graph.knowledge"]["status"],
        "applied"
    );
}

#[test]
fn cluster_e2e_payload_drift_self_heals() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    init_cluster_derived_graph(temp.path());
    let import = cluster_json(temp.path(), "import");
    assert_eq!(import["ok"], true, "{import}");
    let apply = cluster_json(temp.path(), "apply");
    assert_eq!(apply["converged"], true, "{apply}");

    let query_digest = change_for(&apply, "query.knowledge.find_person")["after_digest"]
        .as_str()
        .unwrap()
        .to_string();
    let blob = temp
        .path()
        .join("__cluster/resources/query/knowledge/find_person")
        .join(format!("{query_digest}.gq"));
    fs::remove_file(&blob).unwrap();

    let status = cluster_json(temp.path(), "status");
    assert_eq!(status["ok"], true, "{status}");
    assert!(
        status["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|diagnostic| diagnostic["code"] == "catalog_payload_missing"),
        "{status}"
    );

    let refresh = cluster_json(temp.path(), "refresh");
    assert_eq!(refresh["ok"], true, "{refresh}");
    assert_eq!(
        refresh["resource_statuses"]["query.knowledge.find_person"]["status"],
        "drifted"
    );

    let heal = cluster_json(temp.path(), "apply");
    assert_eq!(heal["ok"], true, "{heal}");
    assert_eq!(heal["converged"], true, "{heal}");
    assert!(blob.exists(), "blob republished");

    let clean = cluster_json(temp.path(), "status");
    assert!(
        !clean["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|diagnostic| {
                diagnostic["code"]
                    .as_str()
                    .is_some_and(|code| code.starts_with("catalog_payload"))
            }),
        "{clean}"
    );
}
