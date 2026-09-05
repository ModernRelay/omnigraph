//! Cluster command surface: validate/plan/apply/approve/status/sync/force-unlock.
//! Moved verbatim from tests/cli.rs in the modularization.

use std::fs;

use tempfile::tempdir;

mod support;

use support::managed_http::{IntentApiFixture, IntentReply, IntentRequest};
use support::*;

fn managed_envelope(kind: &str, state: &str) -> serde_json::Value {
    let outcome = if ["proposed", "offered", "running"].contains(&state) {
        serde_json::Value::Null
    } else {
        serde_json::json!(state)
    };
    serde_json::json!({
        "data": {"cluster_id":"managed-test", "run_id":"run-one", "kind":kind,
            "state":state, "outcome":outcome, "proposer":"authenticated-actor",
            "plan":{"plan_digest":"exact-plan", "bundle_digest":"exact-bytes"}},
        "meta":{"cluster_id":"managed-test", "incarnation":"inc-one",
            "provenance":"service_db", "assurance":"verified_workload", "stale":false}
    })
}

fn assert_control_request(
    request: &IntentRequest,
    method: &str,
    path: &str,
    body: serde_json::Value,
    key: Option<&str>,
) {
    assert_eq!(request.method, method);
    assert_eq!(request.path, path);
    assert_eq!(request.body, body);
    assert_eq!(
        request.headers.get("authorization").map(String::as_str),
        Some("Bearer og_fixture_control")
    );
    assert_eq!(
        request.headers.get("idempotency-key").map(String::as_str),
        key
    );
}

fn assert_no_core_effects(root: &std::path::Path) {
    assert!(
        !root.join("__cluster").exists(),
        "managed failure opened Core state"
    );
    assert!(
        !root.join("graphs").exists(),
        "managed failure created graph storage"
    );
}

#[test]
fn managed_use_verifies_access_before_writing_context() {
    let temp = tempdir().unwrap();
    let body = serde_json::json!({"data":{"cluster_id":"managed-test","name":"prod"},
        "meta":{"cluster_id":"managed-test","assurance":"verified_workload"}});
    let api = IntentApiFixture::new(vec![IntentReply::json(200, body.clone())]);
    let output = output_success(
        cli()
            .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
            .env("OMNIGRAPH_CONTROL_API", &api.origin)
            .args(["use", "managed-test", "--api"])
            .arg(&api.origin)
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    );
    assert_eq!(parse_stdout_json(&output), body);
    let context: serde_yaml::Value =
        serde_yaml::from_slice(&fs::read(temp.path().join(".omnigraph/context")).unwrap()).unwrap();
    assert_eq!(context["version"], 1);
    assert_eq!(context["cluster"], "managed-test");
    assert_eq!(context["api"].as_str(), Some(api.origin.as_str()));
    let requests = api.requests();
    assert_eq!(requests.len(), 1);
    assert_control_request(
        &requests[0],
        "GET",
        "/v1/clusters/managed-test",
        serde_json::Value::Null,
        None,
    );
    assert_no_core_effects(temp.path());
}

#[test]
fn managed_plan_and_apply_submit_exact_intent_without_waiting() {
    for (kind, arguments, expected) in [
        (
            "plan",
            vec!["plan", "--rev", "pushed-revision"],
            serde_json::json!({"kind":"plan","revision":"pushed-revision"}),
        ),
        (
            "apply",
            vec!["apply", "--plan", "saved-plan"],
            serde_json::json!({"kind":"apply","plan_run":"saved-plan"}),
        ),
    ] {
        let temp = tempdir().unwrap();
        write_cluster_config_fixture(temp.path());
        let body = managed_envelope(
            kind,
            if kind == "plan" {
                "proposed"
            } else {
                "offered"
            },
        );
        let api = IntentApiFixture::new(vec![IntentReply::json(202, body.clone())]);
        write_managed_context(temp.path(), &api.origin);
        let output = output_success(managed_cli(temp.path(), &api.origin).args(arguments).args([
            "--no-wait",
            "--idempotency-key",
            "exact-key",
            "--json",
        ]));
        assert_eq!(parse_stdout_json(&output), body);
        assert!(String::from_utf8_lossy(&output.stderr).contains("exact-key"));
        assert!(!String::from_utf8_lossy(&output.stderr).contains("og_fixture_control"));
        let requests = api.requests();
        assert_eq!(requests.len(), 1);
        assert_control_request(
            &requests[0],
            "POST",
            "/v1/clusters/managed-test/runs",
            expected,
            Some("exact-key"),
        );
        assert_no_core_effects(temp.path());
    }
}

#[test]
fn managed_plan_polls_the_accepted_run_and_timeout_does_not_cancel_it() {
    for timeout in [false, true] {
        let temp = tempdir().unwrap();
        let proposed = managed_envelope("plan", "proposed");
        let converged = managed_envelope("plan", "converged");
        let api = IntentApiFixture::new(vec![
            IntentReply::json(202, proposed.clone()),
            IntentReply::json(200, converged.clone()),
        ]);
        write_managed_context(temp.path(), &api.origin);
        let output = managed_cli(temp.path(), &api.origin)
            .args([
                "plan",
                "--timeout",
                if timeout { "1" } else { "10" },
                "--idempotency-key",
                "poll-key",
                "--json",
            ])
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(if timeout { 5 } else { 0 }),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            parse_stdout_json(&output),
            if timeout { proposed } else { converged }
        );
        let requests = api.requests();
        assert_eq!(requests.len(), if timeout { 1 } else { 2 });
        assert_control_request(
            &requests[0],
            "POST",
            "/v1/clusters/managed-test/runs",
            serde_json::json!({"kind":"plan"}),
            Some("poll-key"),
        );
        if !timeout {
            assert_control_request(
                &requests[1],
                "GET",
                "/v1/runs/run-one",
                serde_json::Value::Null,
                None,
            );
        }
        assert_no_core_effects(temp.path());
    }
}

#[test]
fn managed_terminal_outcomes_and_http_refusals_preserve_json_and_exit_codes() {
    for (state, code) in [
        ("converged", 0),
        ("failed", 1),
        ("refused", 2),
        ("blocked", 2),
        ("partially_converged", 3),
        ("recovery_required", 4),
        ("stalled", 5),
        ("cancelled", 6),
    ] {
        let temp = tempdir().unwrap();
        let body = managed_envelope("apply", state);
        let api = IntentApiFixture::new(vec![IntentReply::json(202, body.clone())]);
        write_managed_context(temp.path(), &api.origin);
        let output = managed_cli(temp.path(), &api.origin)
            .args(["apply", "--plan", "saved-plan", "--json"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(code), "state {state}");
        assert_eq!(parse_stdout_json(&output), body);
        assert_eq!(api.requests().len(), 1);
        assert_no_core_effects(temp.path());
    }
    let temp = tempdir().unwrap();
    let problem = serde_json::json!({"type":"scope_missing","status":403,"detail":"apply denied"});
    let api = IntentApiFixture::new(vec![IntentReply::json(403, problem.clone())]);
    write_managed_context(temp.path(), &api.origin);
    let output = managed_cli(temp.path(), &api.origin)
        .args(["apply", "--plan", "saved-plan", "--json"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(parse_stdout_json(&output), problem);
    assert_eq!(api.requests().len(), 1);
    assert_no_core_effects(temp.path());
}

#[test]
fn managed_invalid_context_refuses_without_network_or_core_effects() {
    let api = IntentApiFixture::new(vec![]);
    for context in [
        "{\n".to_string(),
        format!("version: 2\ncluster: managed-test\napi: {}\n", api.origin),
        format!(
            "version: 1\ncluster: managed-test\napi: {}\nunknown: true\n",
            api.origin
        ),
        "x".repeat(16 * 1024 + 1),
    ] {
        let temp = tempdir().unwrap();
        write_cluster_config_fixture(temp.path());
        write_managed_context(temp.path(), &api.origin);
        fs::write(temp.path().join(".omnigraph/context"), context).unwrap();
        let output = managed_cli(temp.path(), &api.origin)
            .args(["apply", "--json"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert_eq!(parse_stdout_json(&output)["type"], "context_invalid");
        assert_no_core_effects(temp.path());
    }
    assert!(api.requests().is_empty());
}

#[test]
#[cfg(unix)]
fn managed_context_links_and_fifo_refuse_without_blocking_or_core_effects() {
    let api = IntentApiFixture::new(vec![]);
    for variant in ["file-link", "dangling-link", "directory-link", "fifo"] {
        let temp = tempdir().unwrap();
        write_cluster_config_fixture(temp.path());
        write_managed_context(temp.path(), &api.origin);
        let context = temp.path().join(".omnigraph/context");
        match variant {
            "file-link" => {
                let target = temp.path().join("actual-context");
                fs::rename(&context, &target).unwrap();
                std::os::unix::fs::symlink(target, &context).unwrap();
            }
            "dangling-link" => {
                fs::remove_file(&context).unwrap();
                std::os::unix::fs::symlink(temp.path().join("missing"), &context).unwrap();
            }
            "directory-link" => {
                let actual = temp.path().join("actual-directory");
                fs::rename(temp.path().join(".omnigraph"), &actual).unwrap();
                std::os::unix::fs::symlink(actual, temp.path().join(".omnigraph")).unwrap();
            }
            "fifo" => {
                fs::remove_file(&context).unwrap();
                assert!(
                    std::process::Command::new("mkfifo")
                        .arg(&context)
                        .status()
                        .unwrap()
                        .success()
                );
            }
            _ => unreachable!(),
        }
        let output = managed_cli(temp.path(), &api.origin)
            .args(["apply", "--json"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{variant}");
        assert_eq!(
            parse_stdout_json(&output)["type"],
            "context_invalid",
            "{variant}"
        );
        assert_no_core_effects(temp.path());
    }
    assert!(api.requests().is_empty());
}

#[test]
fn managed_context_is_exact_directory_and_explicit_direct_preserves_core() {
    let temp = tempdir().unwrap();
    let api = IntentApiFixture::new(vec![]);
    write_cluster_config_fixture(temp.path());
    write_managed_context(temp.path(), &api.origin);
    let unsupported = managed_cli(temp.path(), &api.origin)
        .args(["refresh", "--json"])
        .output()
        .unwrap();
    assert_eq!(unsupported.status.code(), Some(2));
    assert_eq!(
        parse_stdout_json(&unsupported)["type"],
        "managed_command_unsupported"
    );
    assert_no_core_effects(temp.path());
    fs::write(temp.path().join(".omnigraph/context"), "{\n").unwrap();
    let direct = output_success(
        managed_cli(temp.path(), &api.origin).args(["--direct", "validate", "--json"]),
    );
    assert_eq!(parse_stdout_json(&direct)["ok"], true);
    let child = temp.path().join("nested");
    fs::create_dir(&child).unwrap();
    write_cluster_config_fixture(&child);
    let implicit = output_success(managed_cli(&child, &api.origin).args(["validate", "--json"]));
    assert_eq!(parse_stdout_json(&implicit)["ok"], true);
    assert!(api.requests().is_empty());
    assert_no_core_effects(&child);
}

#[test]
fn managed_cancel_checks_selected_cluster_before_any_post() {
    let temp = tempdir().unwrap();
    let mut foreign = managed_envelope("plan", "proposed");
    foreign["data"]["cluster_id"] = serde_json::json!("another-cluster");
    let api = IntentApiFixture::new(vec![IntentReply::json(200, foreign)]);
    write_managed_context(temp.path(), &api.origin);
    let output = managed_cli(temp.path(), &api.origin)
        .args(["cancel", "run-one", "--json"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(parse_stdout_json(&output)["type"], "context_mismatch");
    let requests = api.requests();
    assert_eq!(requests.len(), 1);
    assert_control_request(
        &requests[0],
        "GET",
        "/v1/runs/run-one",
        serde_json::Value::Null,
        None,
    );
    assert_no_core_effects(temp.path());
}

#[test]
fn managed_cancel_selects_pending_cancel_or_completed_plan_abandon() {
    for (before, after, verb, code) in [
        ("proposed", "cancelled", "cancel", 6),
        ("converged", "converged", "abandon", 0),
    ] {
        let temp = tempdir().unwrap();
        let mut result = managed_envelope("plan", after);
        if verb == "abandon" {
            result["data"]["abandoned_at"] = serde_json::json!("2026-09-05T00:00:00Z");
        }
        let api = IntentApiFixture::new(vec![
            IntentReply::json(200, managed_envelope("plan", before)),
            IntentReply::json(200, result.clone()),
        ]);
        write_managed_context(temp.path(), &api.origin);
        let output = managed_cli(temp.path(), &api.origin)
            .args(["cancel", "run-one", "--json"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(code));
        assert_eq!(parse_stdout_json(&output), result);
        let requests = api.requests();
        assert_eq!(requests.len(), 2);
        assert_control_request(
            &requests[0],
            "GET",
            "/v1/runs/run-one",
            serde_json::Value::Null,
            None,
        );
        assert_control_request(
            &requests[1],
            "POST",
            &format!("/v1/runs/run-one:{verb}"),
            serde_json::Value::Null,
            None,
        );
        assert_no_core_effects(temp.path());
    }
}

#[test]
fn managed_redirect_and_oversized_reply_fail_without_following_or_core_effects() {
    let redirect_target = IntentApiFixture::new(vec![]);
    for (headers, status, expected) in [
        (
            vec![(
                "Location".to_string(),
                format!("{}/must-not-receive-token", redirect_target.origin),
            )],
            302,
            "api_redirect_refused",
        ),
        (
            vec![(
                "Content-Length".to_string(),
                (8 * 1024 * 1024 + 1).to_string(),
            )],
            200,
            "api_response_too_large",
        ),
    ] {
        let temp = tempdir().unwrap();
        write_cluster_config_fixture(temp.path());
        let api = IntentApiFixture::new(vec![IntentReply {
            status,
            headers,
            body: vec![],
        }]);
        write_managed_context(temp.path(), &api.origin);
        let output = managed_cli(temp.path(), &api.origin)
            .args(["apply", "--plan", "saved-plan", "--json"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1));
        assert_eq!(parse_stdout_json(&output)["type"], expected);
        assert_eq!(api.requests().len(), 1);
        assert_no_core_effects(temp.path());
    }
    assert!(redirect_target.requests().is_empty());
}

#[test]
fn managed_origin_mismatch_and_api_down_never_open_core() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    let api = IntentApiFixture::new(vec![]);
    write_managed_context(temp.path(), &api.origin);
    let mismatch = managed_cli(temp.path(), &api.origin)
        .env("OMNIGRAPH_CONTROL_API", "https://other.example")
        .args(["apply", "--plan", "saved-plan", "--json"])
        .output()
        .unwrap();
    assert_eq!(mismatch.status.code(), Some(2));
    assert!(api.requests().is_empty());
    assert_no_core_effects(temp.path());
    let origin = api.origin.clone();
    drop(api);
    let outage = managed_cli(temp.path(), &origin)
        .args(["apply", "--plan", "saved-plan", "--json"])
        .output()
        .unwrap();
    assert_eq!(outage.status.code(), Some(1));
    assert_eq!(parse_stdout_json(&outage)["type"], "transport_failed");
    assert_no_core_effects(temp.path());
}

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
fn cluster_validate_rejects_semantically_invalid_policy() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    fs::write(
        temp.path().join("base.policy.yaml"),
        r#"
version: 1
groups:
  team: [act-andrew]
rules:
  - id: invalid-invoke-scope
    allow:
      actors: { group: team }
      actions: [invoke_query]
      branch_scope: any
"#,
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
    assert!(
        stdout.contains("ERROR policy_invalid policies.base.file"),
        "{stdout}"
    );
    assert!(
        stdout.contains("branch_scope") && stdout.contains("invoke_query"),
        "{stdout}"
    );
}

#[test]
fn cluster_validate_rejects_policy_binding_kind_mismatch() {
    for (applies_to, action, scope, expected_kind) in [
        ("knowledge", "graph_list", "", "server-scoped"),
        ("cluster", "read", "      branch_scope: any\n", "per-graph"),
    ] {
        let temp = tempdir().unwrap();
        write_cluster_config_fixture(temp.path());
        let config_path = temp.path().join("cluster.yaml");
        let config = fs::read_to_string(&config_path).unwrap().replace(
            "applies_to: [knowledge]",
            &format!("applies_to: [{applies_to}]"),
        );
        fs::write(config_path, config).unwrap();
        fs::write(
            temp.path().join("base.policy.yaml"),
            format!(
                r#"
version: 1
groups:
  team: [act-andrew]
rules:
  - id: wrong-kind
    allow:
      actors: {{ group: team }}
      actions: [{action}]
{scope}"#
            ),
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
        assert!(
            stdout.contains("ERROR policy_invalid policies.base.file"),
            "{stdout}"
        );
        assert!(
            stdout.contains(expected_kind) && stdout.contains(action),
            "{stdout}"
        );
    }

    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    let config_path = temp.path().join("cluster.yaml");
    let config = fs::read_to_string(&config_path).unwrap().replace(
        "applies_to: [knowledge]",
        "applies_to: [cluster, knowledge]",
    );
    fs::write(config_path, config).unwrap();
    let output = output_failure(
        cli()
            .arg("cluster")
            .arg("validate")
            .arg("--config")
            .arg(temp.path()),
    );
    let stdout = stdout_string(&output);
    assert!(
        stdout.contains("ERROR policy_mixed_binding_kinds policies.base.applies_to"),
        "{stdout}"
    );
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
    assert!(json["observations"]["graph.knowledge"]["graph_manifest_version"].is_number());
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
    let seeded_state: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(temp.path().join("__cluster/state.json")).unwrap(),
    )
    .unwrap();
    let seeded_graph = &seeded_state["applied_revision"]["resources"]["graph.knowledge"];
    assert_eq!(seeded_graph["digest"].as_str().unwrap().len(), 64);
    assert!(
        seeded_graph.get("external_blob_policy").is_none(),
        "the fixture must exercise the valid historical missing-policy => Deny shape"
    );

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
        assert!(
            output.status.success(),
            "cluster apply failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let json = parse_stdout_json(&output);
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
    for subcommand in ["import", "apply"] {
        let mut command = cli();
        command
            .current_dir(temp.path())
            .env("OMNIGRAPH_HOME", operator_home.path())
            .arg("cluster")
            .arg(subcommand)
            .arg("--config")
            .arg(temp.path());
        let output = command.output().unwrap();
        assert!(output.status.success(), "cluster {subcommand} failed");
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
    assert!(stderr.contains("operator.actor"), "{stderr}");
    assert!(stderr.contains("config.yaml"), "{stderr}");
    assert!(!stderr.contains("cli.actor"), "{stderr}");
    assert!(!stderr.contains("omnigraph.yaml"), "{stderr}");
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
        assert_eq!(
            a[key], b[key],
            "conflicting omnigraph.yaml leaked into cluster validate ({key})"
        );
    }
    let leaked = b.to_string();
    assert!(
        !leaked.contains("phantom") && !leaked.contains("9999"),
        "{leaked}"
    );
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
        payload["datasets"].as_array().is_some(),
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
        parse_stdout_json(&out)["datasets"].as_array().is_some(),
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
    fs::write(root.join("base.policy.yaml"), "version: 1\nrules: []\n").unwrap();
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
fn schema_apply_refuses_a_cluster_managed_graph_and_signposts_cluster_apply() {
    // RFC-011 Decision 10: a direct `schema apply` against a cluster-managed
    // graph's storage root would bypass the ledger/recovery/approvals, so it is
    // refused and points at `cluster apply` (mirrors `init`'s refusal).
    let temp = applied_knowledge_cluster();
    // A schema that WOULD change the graph (adds `bio`) — so the no-mutation
    // assertion below is meaningful, not a no-op re-apply.
    fs::write(
        temp.path().join("people_v2.pg"),
        "node Person {\n  name: String @key\n  age: I32?\n  bio: String?\n}\n",
    )
    .unwrap();
    let out = output_failure(
        cli()
            .arg("schema")
            .arg("apply")
            .arg("--schema")
            .arg(temp.path().join("people_v2.pg"))
            .arg("--store")
            .arg(temp.path().join("graphs").join("knowledge.omni")),
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("cluster apply"),
        "schema apply against a cluster-managed graph should signpost `cluster apply`; got: {stderr}"
    );
    // And it bailed BEFORE mutating: the live schema still lacks `bio`.
    let show = output_success(
        cli()
            .arg("schema")
            .arg("show")
            .arg(temp.path().join("graphs").join("knowledge.omni")),
    );
    assert!(
        !stdout_string(&show).contains("bio"),
        "the refused apply must not have changed the live schema; got: {}",
        stdout_string(&show)
    );
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
        parse_stdout_json(&out)["datasets"].as_array().is_some(),
        "optimize should resolve via the ledger despite degraded catalog payloads"
    );
}
