//! Cluster command surface: validate/plan/apply/approve/status/sync/force-unlock.
//! Moved verbatim from tests/cli.rs in the modularization.

use std::fs;

use tempfile::tempdir;

mod support;

use support::*;


#[test]
fn cluster_validate_config_success() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());

    let output = output_success(
        cli()
            .arg("cluster")
            .arg("validate")
            .arg("--config")
            .arg(temp.path()),
    );
    let stdout = stdout_string(&output);
    assert!(stdout.contains("cluster config valid"), "{stdout}");
}

#[test]
fn cluster_validate_json_is_stable() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());

    let json = parse_stdout_json(&output_success(
        cli()
            .arg("cluster")
            .arg("validate")
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(json["ok"], true);
    assert!(json["resource_digests"]["graph.knowledge"].is_string());
    assert!(json["resource_digests"]["query.knowledge.find_person"].is_string());
    assert_eq!(json["dependencies"][0]["from"], "policy.base");
    assert_eq!(json["dependencies"][0]["to"], "graph.knowledge");
}

#[test]
fn cluster_plan_json_reads_inferred_local_state() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    let state_dir = temp.path().join("__cluster");
    fs::create_dir_all(&state_dir).unwrap();
    fs::write(
        state_dir.join("state.json"),
        r#"
{
  "version": 1,
  "applied_revision": {
    "config_digest": "old",
    "resources": {
      "graph.knowledge": { "digest": "old-graph" },
      "policy.old": { "digest": "old-policy" }
    }
  }
}
"#,
    )
    .unwrap();

    let json = parse_stdout_json(&output_success(
        cli()
            .arg("cluster")
            .arg("plan")
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(json["ok"], true);
    assert_eq!(json["state_observations"]["state_found"], true);
    assert!(
        json["changes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|change| change["resource"] == "policy.old" && change["operation"] == "delete"),
        "plan should read state and delete stale resources: {json}"
    );
}

#[test]
fn cluster_status_json_reports_missing_state() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());

    let json = parse_stdout_json(&output_success(
        cli()
            .arg("cluster")
            .arg("status")
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(json["ok"], true);
    assert_eq!(json["state_observations"]["state_found"], false);
    assert!(
        json["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|diagnostic| diagnostic["code"] == "state_missing"),
        "missing state should be a warning diagnostic: {json}"
    );
}

#[test]
fn cluster_status_json_reports_lock_metadata() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    write_cluster_lock(temp.path(), "held-lock", "refresh");

    let json = parse_stdout_json(&output_success(
        cli()
            .arg("cluster")
            .arg("status")
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(json["ok"], true);
    assert_eq!(json["state_observations"]["locked"], true);
    assert_eq!(json["state_observations"]["lock_id"], "held-lock");
    assert_eq!(json["state_observations"]["lock_operation"], "refresh");
    assert_eq!(json["state_observations"]["lock_pid"], 123);
    assert_eq!(
        json["state_observations"]["lock_created_at"],
        "1970-01-01T00:00:00Z"
    );
    assert!(json["state_observations"]["lock_age_seconds"].is_number());
}

#[test]
fn cluster_status_json_reports_extended_state() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    let state_dir = temp.path().join("__cluster");
    fs::create_dir_all(&state_dir).unwrap();
    fs::write(
        state_dir.join("state.json"),
        r#"
{
  "version": 1,
  "state_revision": 5,
  "applied_revision": {
    "config_digest": "applied",
    "resources": {
      "graph.knowledge": { "digest": "graph-digest" }
    }
  },
  "resource_statuses": {
    "graph.knowledge": { "status": "applied", "conditions": ["healthy"] }
  },
  "approval_records": {},
  "recovery_records": {},
  "observations": {}
}
"#,
    )
    .unwrap();

    let json = parse_stdout_json(&output_success(
        cli()
            .arg("cluster")
            .arg("status")
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(json["ok"], true);
    assert_eq!(json["state_observations"]["state_revision"], 5);
    assert!(
        json["state_observations"]["state_cas"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    assert_eq!(json["resource_digests"]["graph.knowledge"], "graph-digest");
    assert_eq!(
        json["resource_statuses"]["graph.knowledge"]["status"],
        "applied"
    );
}

#[test]
fn cluster_plan_json_includes_state_cas_revision_and_lock_observation() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    let state_dir = temp.path().join("__cluster");
    fs::create_dir_all(&state_dir).unwrap();
    fs::write(
        state_dir.join("state.json"),
        r#"
{
  "version": 1,
  "state_revision": 9,
  "applied_revision": {
    "config_digest": "old",
    "resources": {
      "graph.knowledge": { "digest": "old-graph" }
    }
  }
}
"#,
    )
    .unwrap();

    let json = parse_stdout_json(&output_success(
        cli()
            .arg("cluster")
            .arg("plan")
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(json["ok"], true);
    assert_eq!(json["state_observations"]["state_revision"], 9);
    assert!(
        json["state_observations"]["state_cas"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    assert_eq!(json["state_observations"]["locked"], false);
    assert_eq!(json["state_observations"]["lock_acquired"], true);
    assert!(json["state_observations"]["acquired_lock_id"].is_string());
    assert!(!state_dir.join("lock.json").exists());
}

#[test]
fn cluster_plan_locked_state_exits_nonzero() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    write_cluster_lock(temp.path(), "held-lock", "plan");

    let output = output_failure(
        cli()
            .arg("cluster")
            .arg("plan")
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    );
    let json = parse_stdout_json(&output);
    assert_eq!(json["ok"], false);
    assert_eq!(json["state_observations"]["locked"], true);
    assert_eq!(json["state_observations"]["lock_acquired"], false);
    assert_eq!(json["state_observations"]["lock_id"], "held-lock");
    assert_eq!(json["state_observations"]["lock_operation"], "plan");
    assert_eq!(json["state_observations"]["lock_pid"], 123);
    assert_eq!(
        json["state_observations"]["lock_created_at"],
        "1970-01-01T00:00:00Z"
    );
    assert!(json["state_observations"]["lock_age_seconds"].is_number());
    assert!(
        json["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|diagnostic| diagnostic["code"] == "state_lock_held"
                && diagnostic["message"]
                    .as_str()
                    .unwrap()
                    .contains("force-unlock held-lock")),
        "locked state should produce a useful diagnostic: {json}"
    );
}

#[test]
fn cluster_force_unlock_json_removes_lock() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    write_cluster_lock(temp.path(), "held-lock", "plan");

    let json = parse_stdout_json(&output_success(
        cli()
            .arg("cluster")
            .arg("force-unlock")
            .arg("held-lock")
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(json["ok"], true);
    assert_eq!(json["lock_removed"], true);
    assert_eq!(json["state_observations"]["lock_id"], "held-lock");
    assert_eq!(json["state_observations"]["lock_operation"], "plan");
    assert!(!temp.path().join("__cluster/lock.json").exists());
}

#[test]
fn cluster_force_unlock_wrong_id_exits_nonzero() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    write_cluster_lock(temp.path(), "held-lock", "plan");

    let json = parse_stdout_json(&output_failure(
        cli()
            .arg("cluster")
            .arg("force-unlock")
            .arg("other-lock")
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(json["ok"], false);
    assert_eq!(json["lock_removed"], false);
    assert!(
        json["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|diagnostic| diagnostic["code"] == "state_lock_id_mismatch")
    );
    assert!(temp.path().join("__cluster/lock.json").exists());
}

#[test]
fn cluster_locked_plan_then_force_unlock_then_plan_succeeds() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    write_cluster_lock(temp.path(), "held-lock", "plan");

    let locked = parse_stdout_json(&output_failure(
        cli()
            .arg("cluster")
            .arg("plan")
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(locked["ok"], false);
    assert_eq!(locked["state_observations"]["lock_id"], "held-lock");

    let unlocked = parse_stdout_json(&output_success(
        cli()
            .arg("cluster")
            .arg("force-unlock")
            .arg("held-lock")
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(unlocked["lock_removed"], true);

    let planned = parse_stdout_json(&output_success(
        cli()
            .arg("cluster")
            .arg("plan")
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(planned["ok"], true);
}

#[test]
fn cluster_import_json_bootstraps_missing_state() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    init_cluster_derived_graph(temp.path());

    let json = parse_stdout_json(&output_success(
        cli()
            .arg("cluster")
            .arg("import")
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(json["ok"], true);
    assert_eq!(json["operation"], "import");
    assert_eq!(json["state_observations"]["state_revision"], 1);
    assert!(
        json["state_observations"]["state_cas"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    assert_eq!(json["state_observations"]["locked"], false);
    assert_eq!(json["state_observations"]["lock_acquired"], true);
    assert!(json["state_observations"]["acquired_lock_id"].is_string());
    assert!(json["observations"]["graph.knowledge"]["manifest_version"].is_number());
    assert_eq!(
        json["resource_statuses"]["graph.knowledge"]["status"],
        "applied"
    );
    assert!(temp.path().join("__cluster/state.json").exists());
    assert!(!temp.path().join("__cluster/lock.json").exists());
}

#[test]
fn cluster_refresh_json_updates_revision_cas_and_removes_lock() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    init_cluster_derived_graph(temp.path());
    let state_dir = temp.path().join("__cluster");
    fs::create_dir_all(&state_dir).unwrap();
    fs::write(
        state_dir.join("state.json"),
        r#"
{
  "version": 1,
  "state_revision": 2,
  "applied_revision": { "resources": {} }
}
"#,
    )
    .unwrap();

    let json = parse_stdout_json(&output_success(
        cli()
            .arg("cluster")
            .arg("refresh")
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(json["ok"], true);
    assert_eq!(json["operation"], "refresh");
    assert_eq!(json["state_observations"]["state_revision"], 3);
    assert!(
        json["state_observations"]["state_cas"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    assert_eq!(json["state_observations"]["locked"], false);
    assert_eq!(json["state_observations"]["lock_acquired"], true);
    assert!(json["state_observations"]["acquired_lock_id"].is_string());
    assert!(!state_dir.join("lock.json").exists());
}

#[test]
fn cluster_refresh_missing_state_exits_nonzero() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());

    let output = output_failure(
        cli()
            .arg("cluster")
            .arg("refresh")
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    );
    let json = parse_stdout_json(&output);
    assert_eq!(json["ok"], false);
    assert!(
        json["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|diagnostic| diagnostic["code"] == "state_missing"),
        "missing state should produce a useful diagnostic: {json}"
    );
}

#[test]
fn cluster_import_existing_state_exits_nonzero() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    let state_dir = temp.path().join("__cluster");
    fs::create_dir_all(&state_dir).unwrap();
    fs::write(
        state_dir.join("state.json"),
        r#"{"version":1,"applied_revision":{"resources":{}}}"#,
    )
    .unwrap();

    let output = output_failure(
        cli()
            .arg("cluster")
            .arg("import")
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    );
    let json = parse_stdout_json(&output);
    assert_eq!(json["ok"], false);
    assert!(
        json["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|diagnostic| diagnostic["code"] == "state_already_exists"),
        "existing state should produce a useful diagnostic: {json}"
    );
}

#[test]
fn cluster_refresh_and_import_locked_state_exit_nonzero() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    let state_dir = temp.path().join("__cluster");
    fs::create_dir_all(&state_dir).unwrap();
    fs::write(
        state_dir.join("state.json"),
        r#"{"version":1,"applied_revision":{"resources":{}}}"#,
    )
    .unwrap();
    fs::write(
        state_dir.join("lock.json"),
        r#"{"version":1,"lock_id":"held-lock","operation":"refresh","created_at":"2026-06-08T00:00:00Z","pid":123}"#,
    )
    .unwrap();

    let refresh = parse_stdout_json(&output_failure(
        cli()
            .arg("cluster")
            .arg("refresh")
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(refresh["state_observations"]["locked"], true);
    assert_eq!(refresh["state_observations"]["lock_id"], "held-lock");
    assert_eq!(refresh["state_observations"]["lock_acquired"], false);
    assert!(
        refresh["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|diagnostic| diagnostic["code"] == "state_lock_held")
    );

    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    let state_dir = temp.path().join("__cluster");
    fs::create_dir_all(&state_dir).unwrap();
    fs::write(
        state_dir.join("lock.json"),
        r#"{"version":1,"lock_id":"held-lock","operation":"import","created_at":"2026-06-08T00:00:00Z","pid":123}"#,
    )
    .unwrap();

    let imported = parse_stdout_json(&output_failure(
        cli()
            .arg("cluster")
            .arg("import")
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(imported["state_observations"]["locked"], true);
    assert_eq!(imported["state_observations"]["lock_id"], "held-lock");
    assert_eq!(imported["state_observations"]["lock_acquired"], false);
    assert!(
        imported["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|diagnostic| diagnostic["code"] == "state_lock_held")
    );
}

#[test]
fn cluster_validate_invalid_config_exits_nonzero() {
    let temp = tempdir().unwrap();
    fs::write(
        temp.path().join("cluster.yaml"),
        "version: 1\ngraphs: {}\npipelines: {}\n",
    )
    .unwrap();

    let output = output_failure(
        cli()
            .arg("cluster")
            .arg("validate")
            .arg("--config")
            .arg(temp.path()),
    );
    let stdout = stdout_string(&output);
    assert!(stdout.contains("future_phase_field"), "{stdout}");
}

#[test]
fn cluster_apply_json_applies_query_and_policy() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    let validate = write_cluster_applyable_state(temp.path());

    let json = parse_stdout_json(&output_success(
        cli()
            .arg("cluster")
            .arg("apply")
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(json["ok"], true, "{json}");
    assert_eq!(json["applied_count"], 2, "{json}");
    assert_eq!(json["converged"], true, "{json}");
    assert_eq!(json["state_written"], true, "{json}");
    assert_eq!(
        json["resource_statuses"]["query.knowledge.find_person"]["status"],
        "applied"
    );

    let query_digest = validate["resource_digests"]["query.knowledge.find_person"]
        .as_str()
        .unwrap();
    let payload = temp
        .path()
        .join("__cluster/resources/query/knowledge/find_person")
        .join(format!("{query_digest}.gq"));
    assert!(payload.exists(), "missing payload {}", payload.display());

    let state: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(temp.path().join("__cluster/state.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(state["state_revision"], 2);
    assert_eq!(
        state["applied_revision"]["resources"]["query.knowledge.find_person"]["digest"],
        *query_digest
    );
}

#[test]
fn cluster_apply_missing_state_exits_nonzero() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());

    let output = output_failure(
        cli()
            .arg("cluster")
            .arg("apply")
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    );
    let json = parse_stdout_json(&output);
    assert_eq!(json["ok"], false);
    assert!(
        json["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|diagnostic| diagnostic["code"] == "state_missing"),
        "{json}"
    );
    assert!(!temp.path().join("__cluster/resources").exists());
}

#[test]
fn cluster_apply_locked_exits_nonzero() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    write_cluster_applyable_state(temp.path());
    write_cluster_lock(temp.path(), "held-lock", "plan");

    let output = output_failure(
        cli()
            .arg("cluster")
            .arg("apply")
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    );
    let json = parse_stdout_json(&output);
    assert_eq!(json["ok"], false);
    assert!(
        json["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|diagnostic| diagnostic["code"] == "state_lock_held"),
        "{json}"
    );
    assert!(temp.path().join("__cluster/lock.json").exists());
    assert!(!temp.path().join("__cluster/resources").exists());
}

/// RFC-011: the actor chain is `--as` > `operator.actor` > none. The CLI no
/// longer reads omnigraph.yaml `cli.actor`.
#[test]
fn cluster_apply_uses_operator_actor_from_omnigraph_home() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    let operator_home = tempdir().unwrap();
    fs::write(
        operator_home.path().join("config.yaml"),
        "operator:\n  actor: act-operator\n",
    )
    .unwrap();

    let output = cli()
        .current_dir(temp.path())
        .env("OMNIGRAPH_HOME", operator_home.path())
        .arg("cluster")
        .arg("import")
        .arg("--config")
        .arg(temp.path())
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");

    let apply = |extra: &[&str]| {
        let mut command = cli();
        command
            .current_dir(temp.path())
            .env("OMNIGRAPH_HOME", operator_home.path());
        for arg in extra {
            command.arg(arg);
        }
        let output = command
            .arg("cluster")
            .arg("apply")
            .arg("--config")
            .arg(temp.path())
            .arg("--json")
            .output()
            .unwrap();
        let json: serde_json::Value =
            serde_json::from_str(String::from_utf8_lossy(&output.stdout).trim()).unwrap();
        json["actor"].clone()
    };

    // No --as: the operator identity applies.
    assert_eq!(
        apply(&[]),
        "act-operator",
        "operator.actor is the no-flag default"
    );
    // --as still wins over the operator layer.
    assert_eq!(apply(&["--as", "andrew"]), "andrew");
}

#[test]
fn cluster_approve_uses_operator_actor_fallback() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    let operator_home = tempdir().unwrap();
    fs::write(
        operator_home.path().join("config.yaml"),
        "operator:\n  actor: act-operator\n",
    )
    .unwrap();
    // Converge, then remove the graph so a gated delete is pending.
    for command in ["import", "apply"] {
        let output = cli()
            .current_dir(temp.path())
            .env("OMNIGRAPH_HOME", operator_home.path())
            .arg("cluster")
            .arg(command)
            .arg("--config")
            .arg(temp.path())
            .output()
            .unwrap();
        assert!(output.status.success(), "cluster {command} failed");
    }
    fs::write(temp.path().join("cluster.yaml"), "version: 1\ngraphs: {}\n").unwrap();

    let output = cli()
        .current_dir(temp.path())
        .env("OMNIGRAPH_HOME", operator_home.path())
        .arg("cluster")
        .arg("approve")
        .arg("graph.knowledge")
        .arg("--config")
        .arg(temp.path())
        .arg("--json")
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let json: serde_json::Value =
        serde_json::from_str(String::from_utf8_lossy(&output.stdout).trim()).unwrap();
    assert_eq!(json["approved_by"], "act-operator");

    // With neither flag nor operator config: refused with the actionable
    // message (an approval without an approver is meaningless).
    let bare = tempdir().unwrap();
    write_cluster_config_fixture(bare.path());
    let bare_home = tempdir().unwrap();
    let output = output_failure(
        cli()
            .current_dir(bare.path())
            .env("OMNIGRAPH_HOME", bare_home.path())
            .arg("cluster")
            .arg("approve")
            .arg("graph.knowledge")
            .arg("--config")
            .arg(bare.path()),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--as"), "{stderr}");
}

#[test]
fn cluster_commands_ignore_legacy_omnigraph_yaml() {
    // RFC-011: the CLI never reads omnigraph.yaml for cluster commands — a
    // present (even malformed) legacy file is inert. The actor falls back to
    // `operator.actor`, then to none (no loud failure on absence).
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    fs::write(temp.path().join("omnigraph.yaml"), "{{{{ not yaml").unwrap();

    for command in ["validate", "plan", "status"] {
        let output = cli()
            .current_dir(temp.path())
            .arg("cluster")
            .arg(command)
            .arg("--config")
            .arg(temp.path())
            .arg("--json")
            .output()
            .unwrap();
        assert!(
            output.status.success() || command == "plan", // plan warns state-missing pre-import; still must not config-error
            "cluster {command} affected by malformed omnigraph.yaml: {output:?}"
        );
        assert!(
            !String::from_utf8_lossy(&output.stderr).contains("omnigraph.yaml"),
            "cluster {command} touched omnigraph.yaml"
        );
    }
    // import + apply (no --as, no operator config): the legacy file is never
    // loaded and the no-actor apply succeeds (actor defaults to none).
    for command in ["import", "apply"] {
        let output = cli()
            .current_dir(temp.path())
            .arg("cluster")
            .arg(command)
            .arg("--config")
            .arg(temp.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "cluster {command} affected by malformed omnigraph.yaml: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn cluster_commands_ignore_conflicting_local_config() {
    let baseline = tempdir().unwrap();
    write_cluster_config_fixture(baseline.path());
    let with_config = tempdir().unwrap();
    write_cluster_config_fixture(with_config.path());
    fs::write(
        with_config.path().join("omnigraph.yaml"),
        r#"
server:
  bind: 0.0.0.0:9999
graphs:
  phantom:
    uri: ./phantom.omni
"#,
    )
    .unwrap();

    let validate = |dir: &std::path::Path| {
        let output = cli()
            .current_dir(dir)
            .arg("cluster")
            .arg("validate")
            .arg("--config")
            .arg(dir)
            .arg("--json")
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        serde_json::from_str::<serde_json::Value>(String::from_utf8_lossy(&output.stdout).trim())
            .unwrap()
    };
    let (a, b) = (validate(baseline.path()), validate(with_config.path()));
    // Compare the path-free invariants (paths embed each tempdir).
    for key in ["ok", "diagnostics", "resource_digests", "dependencies"] {
        assert_eq!(a[key], b[key], "conflicting omnigraph.yaml leaked into cluster validate ({key})");
    }
    let leaked = b.to_string();
    assert!(!leaked.contains("phantom") && !leaked.contains("9999"), "{leaked}");
}


// ── RFC-010 Slice 3: cluster-managed maintenance addressing + init signpost ──

/// Stand up an applied, served cluster with the `knowledge` graph and return
/// its directory guard. Mirrors the e2e setup (fixture → init → import → apply).
fn applied_knowledge_cluster() -> tempfile::TempDir {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    init_cluster_derived_graph(temp.path());
    let import = cluster_json(temp.path(), "import");
    assert_eq!(import["ok"], true, "{import}");
    let apply = cluster_json(temp.path(), "apply");
    assert_eq!(apply["converged"], true, "{apply}");
    temp
}

#[test]
fn optimize_resolves_a_cluster_graph_by_id() {
    let temp = applied_knowledge_cluster();
    // No hand-typed storage path: address the graph by cluster dir + id.
    let out = output_success(
        cli()
            .arg("optimize")
            .arg("--cluster")
            .arg(temp.path())
            .arg("--graph")
            .arg("knowledge")
            .arg("--json"),
    );
    let payload = parse_stdout_json(&out);
    assert!(
        payload["tables"].as_array().is_some(),
        "optimize did not run against the resolved cluster graph: {payload}"
    );
}

#[test]
fn optimize_unknown_cluster_graph_id_errors() {
    let temp = applied_knowledge_cluster();
    let out = output_failure(
        cli()
            .arg("optimize")
            .arg("--cluster")
            .arg(temp.path())
            .arg("--graph")
            .arg("does-not-exist")
            .arg("--json"),
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("is not applied in cluster") && stderr.contains("cluster apply"),
        "expected an unapplied-graph error pointing at cluster apply; got: {stderr}"
    );
}

#[test]
fn optimize_auto_uses_the_sole_cluster_graph() {
    // RFC-011 D7: a cluster with exactly one applied graph needs no --graph —
    // the resolver enumerates the catalog and uses the only candidate.
    let temp = applied_knowledge_cluster();
    let out = output_success(
        cli()
            .arg("optimize")
            .arg("--cluster")
            .arg(temp.path())
            .arg("--json"),
    );
    assert!(
        parse_stdout_json(&out)["tables"].as_array().is_some(),
        "optimize should auto-resolve the sole cluster graph"
    );
}

/// Stand up an applied cluster with two graphs (`knowledge`, `archive`).
fn applied_two_graph_cluster() -> tempfile::TempDir {
    let temp = tempdir().unwrap();
    let root = temp.path();
    fs::write(
        root.join("people.pg"),
        "node Person {\n  name: String @key\n  age: I32?\n}\n",
    )
    .unwrap();
    fs::write(root.join("base.policy.yaml"), "rules: []\n").unwrap();
    fs::write(
        root.join("cluster.yaml"),
        r#"
version: 1
metadata:
  name: two-graph
state:
  backend: cluster
  lock: true
graphs:
  knowledge:
    schema: ./people.pg
  archive:
    schema: ./people.pg
policies:
  base:
    file: ./base.policy.yaml
    applies_to: [knowledge, archive]
"#,
    )
    .unwrap();
    init_named_cluster_graph(root, "knowledge", "people.pg");
    init_named_cluster_graph(root, "archive", "people.pg");
    assert_eq!(cluster_json(root, "import")["ok"], true);
    assert_eq!(cluster_json(root, "apply")["converged"], true);
    temp
}

#[test]
fn optimize_on_multi_graph_cluster_without_graph_lists_candidates() {
    // RFC-011 D7: >1 graph and no --graph → error naming every candidate,
    // never an auto-pick.
    let temp = applied_two_graph_cluster();
    let out = output_failure(
        cli()
            .arg("optimize")
            .arg("--cluster")
            .arg(temp.path())
            .arg("--json"),
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("2 graphs")
            && stderr.contains("archive")
            && stderr.contains("knowledge")
            && stderr.contains("--graph <id>"),
        "expected a candidate-listing error; got: {stderr}"
    );
}

#[test]
fn init_refuses_a_cluster_managed_path_and_signposts_cluster_apply() {
    let temp = applied_knowledge_cluster();
    // Hand-init a NEW graph into the established cluster's storage layout.
    let out = output_failure(
        cli()
            .arg("init")
            .arg("--schema")
            .arg(temp.path().join("people.pg"))
            .arg(temp.path().join("graphs").join("sneaky.omni")),
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("cluster apply"),
        "init into a cluster-managed path should signpost `cluster apply`; got: {stderr}"
    );
    // And it did not create the graph.
    assert!(!temp.path().join("graphs").join("sneaky.omni").exists());
}

#[test]
fn init_outside_a_cluster_still_works() {
    // Regression guard: ordinary init (no cluster layout) is unaffected.
    let temp = tempdir().unwrap();
    let schema = fixture("test.pg");
    let out = output_success(
        cli()
            .arg("init")
            .arg("--schema")
            .arg(&schema)
            .arg(temp.path().join("plain.omni")),
    );
    assert!(stdout_string(&out).contains("initialized"));
}

#[test]
fn optimize_by_cluster_works_when_catalog_payloads_are_degraded() {
    // Robustness (Greptile, #221): maintenance resolves the graph URI from the
    // state ledger alone, so an unrelated corrupt/missing catalog payload (or a
    // pending recovery sweep) does NOT block it — unlike the full serving-snapshot
    // read. This is what keeps `repair --cluster` usable on a degraded cluster.
    let temp = applied_knowledge_cluster();
    // Remove the verified catalog payloads (queries/policies) — a serving read
    // would refuse with a catalog-payload diagnostic; the ledger-only resolve
    // must not care.
    let resources = temp.path().join("__cluster").join("resources");
    if resources.exists() {
        fs::remove_dir_all(&resources).unwrap();
    }
    let out = output_success(
        cli()
            .arg("optimize")
            .arg("--cluster")
            .arg(temp.path())
            .arg("--graph")
            .arg("knowledge")
            .arg("--json"),
    );
    assert!(
        parse_stdout_json(&out)["tables"].as_array().is_some(),
        "optimize should resolve via the ledger despite degraded catalog payloads"
    );
}
