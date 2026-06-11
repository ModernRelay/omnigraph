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

#[test]
fn cluster_apply_uses_cli_actor_from_local_config() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    fs::write(
        temp.path().join("omnigraph.yaml"),
        "cli:\n  actor: act-local\n",
    )
    .unwrap();
    // Phase 1: import once (setup, not under test).
    let output = cli()
        .current_dir(temp.path())
        .arg("cluster")
        .arg("import")
        .arg("--config")
        .arg(temp.path())
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");

    // Phase 2: apply alone, capturing the echoed actor (idempotent re-runs).
    let apply = |extra: &[&str]| {
        let mut command = cli();
        command.current_dir(temp.path());
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
    assert_eq!(apply(&[]), "act-local", "cli.actor is the no-flag default");
    assert_eq!(apply(&["--as", "andrew"]), "andrew", "--as overrides cli.actor");
}

/// RFC-007 PR 1: the operator layer joins the actor chain —
/// `--as` > legacy `cli.actor` (RFC-008 window) > `operator.actor` > none.
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

    // No --as, no omnigraph.yaml: the operator identity applies.
    assert_eq!(
        apply(&[]),
        "act-operator",
        "operator.actor is the no-flag, no-legacy-config default"
    );
    // --as still wins over everything.
    assert_eq!(apply(&["--as", "andrew"]), "andrew");

    // A legacy cli.actor (RFC-008 window) outranks the operator layer.
    fs::write(
        temp.path().join("omnigraph.yaml"),
        "cli:\n  actor: act-legacy\n",
    )
    .unwrap();
    assert_eq!(
        apply(&[]),
        "act-legacy",
        "legacy cli.actor wins over operator.actor during the deprecation window"
    );
}

#[test]
fn cluster_approve_uses_cli_actor_fallback() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    fs::write(
        temp.path().join("omnigraph.yaml"),
        "cli:\n  actor: act-local\n",
    )
    .unwrap();
    // Converge, then remove the graph so a gated delete is pending.
    for command in ["import", "apply"] {
        let output = cli()
            .current_dir(temp.path())
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
    assert_eq!(json["approved_by"], "act-local");

    // With neither flag nor config: refused with the actionable message.
    let bare = tempdir().unwrap();
    write_cluster_config_fixture(bare.path());
    let output = output_failure(
        cli()
            .current_dir(bare.path())
            .arg("cluster")
            .arg("approve")
            .arg("graph.knowledge")
            .arg("--config")
            .arg(bare.path()),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--as"), "{stderr}");
    assert!(stderr.contains("cli.actor"), "{stderr}");
}

#[test]
fn cluster_commands_ignore_malformed_local_config() {
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
    // import + apply with an explicit --as: the config is never loaded.
    for (command, args) in [("import", vec![]), ("apply", vec!["--as", "andrew"])] {
        let mut invocation = cli();
        invocation.current_dir(temp.path());
        for arg in &args {
            invocation.arg(arg);
        }
        let output = invocation
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
    // Only the no-flag actor lookup is allowed to fail, and loudly.
    let output = output_failure(
        cli()
            .current_dir(temp.path())
            .arg("cluster")
            .arg("apply")
            .arg("--config")
            .arg(temp.path()),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("omnigraph.yaml") && stderr.contains("--as"),
        "the actor-default config read must fail loudly and actionably: {stderr}"
    );
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

