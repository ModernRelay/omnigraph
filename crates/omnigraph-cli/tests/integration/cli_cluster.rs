//! Direct, served, and explicitly selected managed cluster command workflows.

use serde_json::Value;
use std::fs;

use tempfile::tempdir;

use crate::support::managed_http::{IntentApiFixture, IntentReply, IntentRequest};
use crate::support::*;

fn managed_fixture_command(
    root: &std::path::Path,
    origin: &str,
    target: &str,
) -> assert_cmd::Command {
    if !managed_home(root).join("config.yaml").exists() {
        write_managed_target(root, origin);
    }
    managed_command(root, origin, target)
}

fn lifecycle_record(root: &std::path::Path, origin: &str, name: &str) -> std::path::PathBuf {
    use sha2::{Digest, Sha256};
    let identity = serde_json::to_vec(&(origin, "account-one")).unwrap();
    managed_home(root)
        .join("lifecycle")
        .join(format!("{:x}", Sha256::digest(identity)))
        .join(name)
}

const MANAGED_REVISION: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn managed_preview(state: &str) -> Value {
    serde_json::json!({"data":{"preview_id":"saved-plan","state":state,"revision":MANAGED_REVISION,"input_digest":"exact-input","generation":{"state_revision":3}},"meta":{"cluster_id":"managed-test","incarnation":"inc-one","provenance":"service_db"}})
}

fn managed_delivery(delivery: &str) -> Value {
    serde_json::json!({"data":{"deployment_id":"delivery-one","native_deployment_id":"ledger:1:nonce","preview_id":"saved-plan","delivery":delivery,"archive":"pending","native_result_status":null,"attempted_at":null,"observation":{"state":"not_observed"}},"meta":{"cluster_id":"managed-test","incarnation":"inc-one","provenance":"service_db"}})
}

fn managed_complete(converged: bool, active: bool, archive: &str) -> Value {
    let mut body = managed_delivery("dispatched");
    body["data"]["archive"] = serde_json::json!(archive);
    body["data"]["native_result_status"] = serde_json::json!("complete");
    body["data"]["observation"] = serde_json::json!({"state":"observed","historical":false,"native":{"deployment":{"status":"complete","result":{"id":"ledger:1:nonce","converged":converged,"graphs":{}}},"active":active,"in_progress":false}});
    body
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

fn lifecycle_envelope(kind: &str, state: &str, phase: &str) -> serde_json::Value {
    serde_json::json!({"data":{"cluster_id":"managed-test","incarnation":"inc-one","operation_id":"operation-one","kind":kind,"state":state,"canonical_root":"s3://tenant/clusters/managed-test-inc-one","lifecycle":{"phase":phase,"purgeAfter":"2026-09-07T12:00:00Z"}},"meta":{"cluster_id":"managed-test","incarnation":"inc-one","provenance":"service_db","assurance":"verified_workload"}})
}

fn lifecycle_session(principal: &str, account: &str) -> serde_json::Value {
    serde_json::json!({"data":{"principal_id":principal,"account_id":account},"meta":{"provenance":"service_db","assurance":"verified_workload"}})
}

fn lifecycle_fixture(replies: Vec<IntentReply>) -> IntentApiFixture {
    IntentApiFixture::with_session(replies, lifecycle_session("principal-one", "account-one"))
}

/// `--as` on a served cluster command asserts the actor; the server refuses
/// another one before any effect, so the command exits 4 having sent nothing
/// that could change the cluster, with and without `--json`.
#[test]
fn as_on_a_served_cluster_command_exits_4_on_another_actor() {
    let refusal = serde_json::json!({
        "error": "the credential resolves to actor 'act-real', not the expected 'act-nope'",
        "code": "actor_mismatch"
    });
    let root = tempdir().unwrap();
    write_cluster_config_fixture(root.path());
    for command in ["plan", "apply"] {
        for json in [true, false] {
            let api = IntentApiFixture::graph(vec![IntentReply::json(412, refusal.clone())]);
            let mut cmd = cli();
            cmd.args(["cluster", command, "--target"])
                .fixture_target(&api.origin)
                .args(["--as", "act-nope", "--config"])
                .arg(root.path());
            if json {
                cmd.arg("--json");
            }
            let output = cmd.output().unwrap();
            assert_eq!(
                output.status.code(),
                Some(4),
                "{command} json={json}: {output:?}"
            );
            if json {
                assert_eq!(parse_stdout_json(&output)["code"], "actor_mismatch");
            } else {
                assert!(
                    String::from_utf8_lossy(&output.stderr).contains("act-nope"),
                    "{command}: {output:?}"
                );
            }
            let requests = api.workflow_requests();
            assert_eq!(requests.len(), 1, "{command}: {requests:?}");
            assert_eq!(requests[0].method, "GET", "{command}: {requests:?}");
            assert_eq!(
                requests[0].headers["omnigraph-if-actor"], "act-nope",
                "{command}: {requests:?}"
            );
            api.assert_complete();
        }
    }
}

#[test]
fn core_remote_status_uses_only_the_selected_target_credential() {
    let ca = tls_gateway::ca_path().display();
    let status = serde_json::json!({"status":{"canonical_root":"file:///server-only-storage","ledger_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","state_revision":1,"result_revision":0,"next_sequence":1,"lock_id":"server-owner","outstanding_id":null,"lookup":null},"active":false,"in_progress":false});
    for mode in ["named", "named_anonymous"] {
        let api = IntentApiFixture::graph(vec![IntentReply::json(200, status.clone())]);
        let operator = KeyedServer::new(&api.origin);
        let mut command = cli();
        command.envs(operator.env("keyed-status-token"));
        if mode == "named_anonymous" {
            command.env("OMNIGRAPH_HOME", tls_gateway::anonymous_home(&api.origin));
        }
        let output = output_success(
            command
                .env("OMNIGRAPH_BEARER_TOKEN", "must-not-leak")
                .args(["cluster", "status", "--target", KEYED_SERVER_NAME, "--json"]),
        );
        assert_eq!(parse_stdout_json(&output), status);
        let requests = api.workflow_requests();
        assert_eq!(requests.len(), 1, "{requests:?}");
        assert_eq!(requests[0].method, "GET");
        assert_eq!(requests[0].path, "/cluster/deployments");
        assert_eq!(
            requests[0].headers.get("authorization").map(String::as_str),
            (mode != "named_anonymous").then_some("Bearer keyed-status-token")
        );
        assert_no_core_effects(operator.home());
        api.assert_complete();
    }
    // Inline served selectors no longer inherit registered credentials and
    // cannot dispatch anonymously either. Keep these refusals separate from
    // the named-target credential assertions above.
    for registered in [false, true] {
        let api = IntentApiFixture::graph(vec![]);
        let operator = KeyedServer::new(&api.origin);
        let mut command = cli();
        if registered {
            command.envs(operator.env("must-not-leak"));
        }
        let refused =
            output_failure(command.args(["cluster", "status", "--target", &api.origin, "--json"]));
        assert!(
            String::from_utf8_lossy(&refused.stderr)
                .contains("inline access supports local storage only"),
            "{refused:?}"
        );
        assert!(api.requests().is_empty());
        assert_no_core_effects(operator.home());
        api.assert_complete();
    }
    // A complete request path may cross a more-specific credential boundary
    // even though the selected server root belongs to the keyed owner.
    for suffix in ["cluster", "cluster/deployments"] {
        let api = IntentApiFixture::graph(vec![]);
        let operator = KeyedServer::new(&api.origin);
        let config_path = operator.home().join("config.yaml");
        let config = fs::read_to_string(&config_path).unwrap();
        fs::write(
            &config_path,
            format!(
                "{config}  nested:\n    kind: server\n    transport:\n      tls:\n        ca: {{mode: replace, file: '{ca}'}}\n    endpoint: '{}/{}'\n    auth:\n      provider: anonymous\n",
                api.origin, suffix
            ),
        )
        .unwrap();
        let output = output_failure(cli().envs(operator.env("must-not-cross-boundary")).args([
            "cluster",
            "status",
            "--target",
            KEYED_SERVER_NAME,
            "--json",
        ]));
        assert_eq!(
            parse_stdout_json(&output)["code"],
            "credential_scope_mismatch"
        );
        assert!(
            api.requests().is_empty(),
            "no discovery or credential may be sent"
        );
        api.assert_complete();
    }
}

#[test]
fn core_remote_retired_selectors_refuse_before_operator_or_backend_access() {
    let home = tempdir().unwrap();
    fs::write(home.path().join("config.yaml"), "targets: [\n").unwrap();
    let api = IntentApiFixture::graph(vec![]);
    for selector in ["--server", "--store", "--cluster", "--profile"] {
        let output = output_failure(cli().env("OMNIGRAPH_HOME", home.path()).args([
            "cluster",
            "status",
            selector,
            &api.origin,
            "--json",
        ]));
        let (exit, diagnostic) = if selector == "--profile" {
            (1, "cluster does not accept --profile")
        } else {
            (64, "unexpected argument")
        };
        assert_eq!(output.status.code(), Some(exit), "{output:?}");
        assert!(output.stdout.is_empty(), "{output:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(diagnostic),
            "{output:?}"
        );
    }
    assert!(api.requests().is_empty());
    assert_no_core_effects(home.path());
    api.assert_complete();
}

/// The source directory belongs to the caller; the file storage root belongs
/// to the server. An absent client-side mount must not prevent HTTP submission.
#[test]
#[ignore = "nightly: six live-apply modes on the real 500 ms poll interval and a 1 s --timeout"]
fn core_live_apply_captures_server_file_root_without_local_storage_access() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    let remote_path = temp
        .path()
        .canonicalize()
        .unwrap()
        .join("server-only-storage");
    let remote_root = format!("file://{}", remote_path.display());
    let config = temp.path().join("cluster.yaml");
    let source = fs::read_to_string(&config).unwrap();
    fs::write(&config, format!("storage: {remote_root}\n{source}")).unwrap();
    let id = "01ARZ3NDEKTSV4RRFFQ69G5FAV:1:01ARZ3NDEKTSV4RRFFQ69G5FAW";
    let status = serde_json::json!({
        "status": {"canonical_root":remote_root, "ledger_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "state_revision":1, "result_revision":0, "next_sequence":1,
            "lock_id":"server-owner", "outstanding_id":null, "lookup":null},
        "active":false, "in_progress":false
    });
    let schema = fs::read_to_string(temp.path().join("people.pg")).unwrap();
    for declared_storage in [Some(remote_root.as_str()), None] {
        let captured_config = match declared_storage {
            Some(root) => format!("storage: {root}\n{source}"),
            None => source.clone(),
        };
        fs::write(&config, &captured_config).unwrap();
        let api = IntentApiFixture::graph(vec![
            IntentReply::json(200, status.clone()),
            // This fixture owns only the client boundary. A typed response proves
            // POST reached it; actual activation remains the process E2E's job.
            IntentReply::json(
                409,
                serde_json::json!({"error":"deployment_fixture_reached", "code":"conflict"}),
            ),
        ]);
        let operator = KeyedServer::new(&api.origin);
        let output = output_failure(
            cli()
                .envs(operator.env("capture-token"))
                .args([
                    "cluster",
                    "apply",
                    "--target",
                    KEYED_SERVER_NAME,
                    "--deployment-id",
                    id,
                    "--config",
                ])
                .arg(temp.path())
                .arg("--json"),
        );
        assert_eq!(
            parse_stdout_json(&output)["error"],
            "deployment_fixture_reached",
            "{output:?}"
        );
        let requests = api.workflow_requests();
        assert_eq!(requests.len(), 2, "{requests:?}");
        assert_eq!(
            (&*requests[0].method, &*requests[0].path),
            ("GET", "/cluster/deployments")
        );
        assert_eq!(
            (&*requests[1].method, &*requests[1].path),
            ("POST", "/cluster/deployments")
        );
        for request in &requests {
            assert_eq!(
                request.headers.get("authorization").map(String::as_str),
                Some("Bearer capture-token")
            );
        }
        assert_eq!(requests[1].body["deployment_id"], id);
        assert!(requests[1].body["deployment"].get("options").is_none());
        assert_eq!(
            requests[1].body["deployment"]["canonical_root"],
            remote_root
        );
        assert!(
            requests[1].body["deployment"]["sources"]
                .as_object()
                .unwrap()
                .values()
                .any(|value| value.as_str() == Some(schema.as_str()))
        );
        assert!(!remote_path.exists());
        assert_no_core_effects(temp.path());
        api.assert_complete();
    }

    // Submission is once-only: acceptance and later activation are distinct
    // observations. Lost delivery permits GETs under the original identity.
    let input_digest = omnigraph_cluster::capture_deployment_for_server(temp.path(), &remote_root)
        .unwrap()
        .input_digest()
        .unwrap();
    let accepted = serde_json::json!({
        "deployment":{"status":"outstanding", "id":id, "input_digest":input_digest, "graphs":{"knowledge":"started"}},
        "active":false, "in_progress":true,
    });
    let complete = serde_json::json!({
        "deployment":{"status":"complete", "result":{
            "id":id, "input_digest":input_digest, "authority":{"kind":"authenticated_identity","actor":"operator"},
            "base":{"result_revision":0,"resource_digests":{},"schema_contracts":{},"capture_cas":"base"},
            "result_revision":1,"config_digest":"desired","graphs":{},"recovery_executors":{},"converged":true,
        }}, "active":true, "in_progress":false,
    });
    for mode in ["accepted", "poll", "lost", "timeout"] {
        let mut replies = vec![IntentReply::json(200, status.clone())];
        replies.push(if mode == "lost" {
            IntentReply::json(504, serde_json::json!({"error":"proxy lost acceptance"}))
        } else {
            IntentReply::json(202, accepted.clone())
        });
        if mode == "lost" {
            replies.push(IntentReply::json(200, serde_json::json!({"deployment":{"status":"not_recorded"},"active":false,"in_progress":true})));
            replies.push(IntentReply::json(200, accepted.clone()));
        }
        if mode == "poll" {
            let mut activating = complete.clone();
            activating["active"] = false.into();
            activating["in_progress"] = true.into();
            replies.push(IntentReply::json(200, activating));
        }
        if mode == "timeout" {
            replies.push(IntentReply::json(200, accepted.clone()));
        } else if mode != "accepted" {
            replies.push(IntentReply::json(200, complete.clone()));
        }
        let api = IntentApiFixture::graph(replies);
        let mut command = cli();
        command
            .args(["cluster", "apply", "--target"])
            .fixture_target(&api.origin)
            .args(["--deployment-id", id, "--config"])
            .arg(temp.path())
            .arg("--json");
        if mode == "accepted" {
            command.args(["--no-wait", "--timeout", "10"]);
        } else {
            command.args(["--timeout", if mode == "timeout" { "1" } else { "10" }]);
        }
        let output = command.output().unwrap();
        let result = parse_stdout_json(&output);
        if mode == "timeout" {
            assert_eq!(output.status.code(), Some(5), "{output:?}");
            assert_eq!(result["outcome"], "wait_timeout");
            assert_eq!(result["deployment_id"], id);
            assert_eq!(result["last_observation"], accepted);
        } else {
            assert!(output.status.success(), "{mode}: {output:?}");
            assert_eq!(
                result,
                if mode == "accepted" {
                    accepted.clone()
                } else {
                    complete.clone()
                }
            );
        }
        let requests = api.workflow_requests();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.method == "POST")
                .count(),
            1
        );
        for request in requests.iter().skip(2) {
            assert_eq!(request.method, "GET");
            assert_eq!(request.path, format!("/cluster/deployments/{id}"));
        }
        assert_no_core_effects(temp.path());
        api.assert_complete();
    }
    // A proxy can lose the input-mismatch refusal for an existing identity.
    // Observing its older receipt must never acknowledge this different input.
    let mut wrong_input_successes = Vec::new();
    for no_wait in [false, true] {
        let mut old_receipt = if no_wait {
            accepted.clone()
        } else {
            complete.clone()
        };
        if no_wait {
            old_receipt["deployment"]["input_digest"] = "0".repeat(64).into();
        } else {
            old_receipt["deployment"]["result"]["input_digest"] = "0".repeat(64).into();
        }
        let api = IntentApiFixture::graph(vec![
            IntentReply::json(200, status.clone()),
            IntentReply::json(
                504,
                serde_json::json!({"error":"proxy lost input-mismatch refusal"}),
            ),
            IntentReply::json(200, old_receipt),
        ]);
        let mut command = cli();
        command
            .args(["cluster", "apply", "--target"])
            .fixture_target(&api.origin)
            .args(["--deployment-id", id, "--config"])
            .arg(temp.path())
            .args(["--json", "--timeout", "10"]);
        if no_wait {
            command.arg("--no-wait");
        }
        let output = command.output().unwrap();
        if output.status.success() {
            wrong_input_successes.push(no_wait);
        } else {
            let error = format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(error.contains("input digest"), "{output:?}");
        }
        let requests = api.workflow_requests();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.method == "POST")
                .count(),
            1
        );
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[2].path, format!("/cluster/deployments/{id}"));
        api.assert_complete();
    }
    assert!(
        wrong_input_successes.is_empty(),
        "different input was incorrectly acknowledged for no_wait={wrong_input_successes:?}"
    );

    // Exact status can wait without loading a config or general cluster status.
    let api = IntentApiFixture::graph(vec![
        IntentReply::json(200, accepted.clone()),
        IntentReply::json(200, complete.clone()),
    ]);
    let observed = output_success(
        cli()
            .args(["cluster", "status", "--target"])
            .fixture_target(&api.origin)
            .args(["--deployment-id", id, "--wait", "--timeout", "10", "--json"]),
    );
    assert_eq!(parse_stdout_json(&observed), complete);
    assert!(
        api.workflow_requests()
            .iter()
            .all(|request| request.method == "GET"
                && request.path == format!("/cluster/deployments/{id}"))
    );
    api.assert_complete();

    // Observation is safe to retry after temporary service refusal or a
    // truncated response. Apply submits once; standalone status only reads.
    for apply in [false, true] {
        let mut truncated = IntentReply::json(200, accepted.clone());
        truncated.headers.push((
            "content-length".into(),
            (truncated.body.len() + 1).to_string(),
        ));
        let mut replies = Vec::new();
        if apply {
            replies.extend([
                IntentReply::json(200, status.clone()),
                IntentReply::json(202, accepted.clone()),
            ]);
        }
        replies.extend([
            IntentReply::json(503, serde_json::json!({"error":"observation changed"})),
            IntentReply::json(429, serde_json::json!({"error":"observation throttled"})),
            truncated,
            IntentReply::json(200, complete.clone()),
        ]);
        let api = IntentApiFixture::graph(replies);
        let mut command = cli();
        command
            .timeout(std::time::Duration::from_secs(15))
            .args([
                "cluster",
                if apply { "apply" } else { "status" },
                "--target",
            ])
            .fixture_target(&api.origin)
            .args(["--deployment-id", id, "--timeout", "10", "--json"]);
        if apply {
            command.arg("--config").arg(temp.path());
        } else {
            command.arg("--wait");
        }
        let observed = parse_stdout_json(&output_success(&mut command));
        assert_eq!(observed, complete);
        let requests = api.workflow_requests();
        assert_eq!(requests.len(), if apply { 6 } else { 4 });
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.method == "POST")
                .count(),
            usize::from(apply),
        );
        for request in requests.iter().skip(if apply { 2 } else { 0 }) {
            assert_eq!(request.method, "GET");
            assert_eq!(request.path, format!("/cluster/deployments/{id}"));
        }
        api.assert_complete();
    }

    // Complete transport bytes with invalid JSON or a malformed receipt are
    // protocol failures, not permission to keep retrying an observation.
    let mut invalid_json = IntentReply::json(200, serde_json::json!({}));
    invalid_json.body = b"{".to_vec();
    for reply in [invalid_json, IntentReply::json(200, serde_json::json!({}))] {
        let api = IntentApiFixture::graph(vec![reply]);
        let output = output_failure(
            cli()
                .args(["cluster", "status", "--target"])
                .fixture_target(&api.origin)
                .args(["--deployment-id", id, "--wait", "--timeout", "10", "--json"]),
        );
        assert_ne!(output.status.code(), Some(5), "{output:?}");
        let requests = api.workflow_requests();
        assert_eq!(requests.len(), 1, "invalid receipt must not be retried");
        assert_eq!(requests[0].method, "GET");
        assert_eq!(requests[0].path, format!("/cluster/deployments/{id}"));
        api.assert_complete();
    }

    for terminal in [
        "not_recorded",
        "identity_mismatch",
        "different_ledger",
        "result_expired",
        "outstanding",
        "complete",
    ] {
        let response = match terminal {
            "outstanding" => {
                let mut response = accepted.clone();
                response["in_progress"] = false.into();
                response
            }
            "complete" => {
                let mut response = complete.clone();
                response["active"] = false.into();
                response
            }
            "result_expired" => {
                serde_json::json!({"deployment":{"status":terminal,"acceptance":"unknown","outcome":"unknown"},"active":false,"in_progress":false})
            }
            _ => {
                serde_json::json!({"deployment":{"status":terminal},"active":false,"in_progress":false})
            }
        };
        let api = IntentApiFixture::graph(vec![IntentReply::json(200, response.clone())]);
        let output = output_failure(
            cli()
                .args(["cluster", "status", "--target"])
                .fixture_target(&api.origin)
                .args(["--deployment-id", id, "--wait", "--json"]),
        );
        assert_eq!(parse_stdout_json(&output), response);
        api.assert_complete();
    }

    // A different declared root still refuses before POST; the server's status
    // is binding evidence, never permission to silently retarget the bundle.
    fs::write(&config, format!("storage: {remote_root}-other\n{source}")).unwrap();
    let api = IntentApiFixture::graph(vec![IntentReply::json(200, status)]);
    let operator = KeyedServer::new(&api.origin);
    output_failure(
        cli()
            .envs(operator.env("capture-token"))
            .args([
                "cluster",
                "apply",
                "--target",
                KEYED_SERVER_NAME,
                "--config",
            ])
            .arg(temp.path())
            .arg("--json"),
    );
    let requests = api.workflow_requests();
    assert_eq!(requests.len(), 1, "{requests:?}");
    assert_eq!(requests[0].method, "GET");
    assert!(!remote_path.exists());
    assert_no_core_effects(temp.path());
    api.assert_complete();
}

#[test]
fn managed_lifecycle_uncertain_create_reuses_durable_key_and_preserves_context() {
    for (status, body) in [
        (503, serde_json::json!({"type":"response_uncertain"})),
        (
            408,
            serde_json::json!({"type":"gateway_timeout","status":408,"title":"gateway timeout","detail":"upstream timed out"}),
        ),
        (
            400,
            serde_json::json!({"type":"unknown_proxy_error","status":400,"title":"proxy error","detail":"unknown upstream outcome"}),
        ),
        (403, serde_json::json!({"type":"scope_missing"})),
    ] {
        let temp = tempdir().unwrap();
        let accepted = lifecycle_envelope("create", "proposed", "provisioning");
        let api = lifecycle_fixture(vec![
            IntentReply::json(status, body),
            IntentReply::json(
                403,
                serde_json::json!({"type":"scope_missing","status":403,"title":"scope missing","detail":"grant revoked since first attempt"}),
            ),
            IntentReply::json(202, accepted.clone()),
        ]);
        let invoke = |name: &str, key: Option<&str>| {
            let mut command = managed_fixture_command(temp.path(), &api.origin, "managed-account");
            command
                .current_dir(temp.path())
                .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
                .env("OMNIGRAPH_CONTROL_API", &api.origin)
                .args(["cluster", "create", name, "--no-wait", "--json"]);
            if let Some(key) = key {
                command.args(["--idempotency-key", key]);
            }
            command.output().unwrap()
        };
        let uncertain = invoke("new-cluster", None);
        assert_eq!(
            uncertain.status.code(),
            Some(if status < 500 { 2 } else { 1 })
        );
        let pending_path = lifecycle_record(temp.path(), &api.origin, "pending-lifecycle.json");
        let pending: serde_json::Value =
            serde_json::from_slice(&fs::read(&pending_path).unwrap()).unwrap();
        assert_eq!(pending["request_digest"].as_str().unwrap().len(), 64);
        assert!(!pending.to_string().contains("og_fixture_control"));
        assert!(!temp.path().join(".omnigraph/context").exists());
        for output in [
            invoke("different-cluster", None),
            invoke("new-cluster", Some("different-key")),
        ] {
            assert_eq!(output.status.code(), Some(2));
            assert_eq!(
                parse_stdout_json(&output)["type"],
                "pending_lifecycle_conflict"
            );
        }
        // Current authorization can change after the first request was accepted.
        // A later permission refusal must not erase that unresolved identity.
        assert_eq!(invoke("new-cluster", None).status.code(), Some(2));
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&fs::read(&pending_path).unwrap()).unwrap(),
            pending
        );
        let retried = invoke("new-cluster", None);
        assert!(retried.status.success(), "{retried:?}");
        assert_eq!(parse_stdout_json(&retried), accepted);
        let requests = api.workflow_requests();
        assert_eq!(requests.len(), 3);
        assert_eq!(
            requests[0].headers["idempotency-key"],
            requests[1].headers["idempotency-key"]
        );
        assert_eq!(
            requests[0].headers["idempotency-key"],
            requests[2].headers["idempotency-key"]
        );
        assert_control_request(
            &requests[1],
            "POST",
            "/v1/clusters",
            serde_json::json!({"name":"new-cluster"}),
            pending["idempotency_key"].as_str(),
        );
        assert!(!pending_path.exists());
        let saved: serde_json::Value = serde_json::from_slice(
            &fs::read(lifecycle_record(
                temp.path(),
                &api.origin,
                "accepted-lifecycle.json",
            ))
            .unwrap(),
        )
        .unwrap();
        assert_eq!(saved[0]["identity"]["operation_id"], "operation-one");
        let context = fs::read(managed_home(temp.path()).join("config.yaml")).unwrap();
        assert_eq!(invoke("new-cluster", None).status.code(), Some(2));
        assert_eq!(
            fs::read(managed_home(temp.path()).join("config.yaml")).unwrap(),
            context
        );
        assert_eq!(api.workflow_requests().len(), 3);
        api.assert_complete();
        assert_no_core_effects(temp.path());
    }
}

#[test]
fn managed_lifecycle_pending_is_principal_bound_across_session_renewal() {
    let temp = tempdir().unwrap();
    let api = lifecycle_fixture(vec![
        IntentReply::json(503, serde_json::json!({"type":"response_uncertain"})),
        IntentReply::json(
            202,
            lifecycle_envelope("create", "proposed", "provisioning"),
        ),
    ]);
    let invoke = |token: &str| {
        managed_fixture_command(temp.path(), &api.origin, "managed-account")
            .current_dir(temp.path())
            .env("OMNIGRAPH_CONTROL_TOKEN", token)
            .env("OMNIGRAPH_CONTROL_API", &api.origin)
            .args(["cluster", "create", "new-name", "--no-wait", "--json"])
            .output()
            .unwrap()
    };
    assert_eq!(invoke("og_fixture_control").status.code(), Some(1));
    let path = lifecycle_record(temp.path(), &api.origin, "pending-lifecycle.json");
    let pending = fs::read(&path).unwrap();
    let record: serde_json::Value = serde_json::from_slice(&pending).unwrap();
    assert_eq!(record["principal_id"], "principal-one");
    assert_eq!(record["account_id"], "account-one");
    for (principal, account) in [
        ("principal-two", "account-one"),
        ("principal-one", "account-two"),
    ] {
        api.set_session(lifecycle_session(principal, account));
        let output = invoke("og_fixture_another_actor");
        assert_eq!(output.status.code(), Some(2));
        assert_eq!(
            parse_stdout_json(&output)["type"],
            if account == "account-one" {
                "pending_lifecycle_conflict"
            } else {
                "organization_mismatch"
            }
        );
        assert_eq!(fs::read(&path).unwrap(), pending);
        assert_eq!(api.workflow_requests().len(), 1);
        assert!(!temp.path().join(".omnigraph/context").exists());
    }
    api.set_session(lifecycle_session("principal-one", "account-one"));
    assert!(invoke("og_fixture_renewed_session").status.success());
    let requests = api.requests();
    assert_eq!(
        requests
            .iter()
            .filter(|r| r.path == "/v1/auth/session")
            .count(),
        4
    );
    for token in ["og_fixture_control", "og_fixture_renewed_session"] {
        let matching: Vec<_> = requests
            .iter()
            .filter(|r| r.headers["authorization"] == format!("Bearer {token}"))
            .collect();
        assert_eq!(matching.len(), 2);
        assert_eq!(
            (matching[0].method.as_str(), matching[0].path.as_str()),
            ("GET", "/v1/auth/session")
        );
        assert_eq!(matching[1].method, "POST");
    }
    let mutations = api.workflow_requests();
    assert_eq!(
        mutations[0].headers["idempotency-key"],
        mutations[1].headers["idempotency-key"]
    );
    assert!(!path.exists());
    api.assert_complete();
}

#[test]
fn managed_lifecycle_definitive_first_refusal_releases_pending_intent() {
    let temp = tempdir().unwrap();
    let api = lifecycle_fixture(vec![
        IntentReply::json(
            409,
            serde_json::json!({"type":"name_taken","status":409,"title":"name taken","detail":"name already exists"}),
        ),
        IntentReply::json(
            202,
            lifecycle_envelope("create", "proposed", "provisioning"),
        ),
    ]);
    let invoke = |name: &str| {
        managed_fixture_command(temp.path(), &api.origin, "managed-account")
            .current_dir(temp.path())
            .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
            .env("OMNIGRAPH_CONTROL_API", &api.origin)
            .args(["cluster", "create", name, "--no-wait", "--json"])
            .output()
            .unwrap()
    };
    assert_eq!(invoke("taken-name").status.code(), Some(2));
    assert!(!lifecycle_record(temp.path(), &api.origin, "pending-lifecycle.json").exists());
    assert!(!temp.path().join(".omnigraph/context").exists());
    assert!(invoke("new-name").status.success());
    let requests = api.workflow_requests();
    assert_eq!(requests.len(), 2);
    assert_ne!(
        requests[0].headers["idempotency-key"],
        requests[1].headers["idempotency-key"]
    );
    api.assert_complete();
}

#[test]
fn managed_lifecycle_delete_and_undo_send_exact_authority_targets() {
    for (kind, args, expected) in [
        (
            "delete",
            vec![
                "delete",
                "--incarnation",
                "inc-one",
                "--retention-seconds",
                "0",
            ],
            serde_json::json!({"incarnation":"inc-one","retention_seconds":0}),
        ),
        (
            "undo",
            vec![
                "undo-delete",
                "--incarnation",
                "inc-one",
                "--deletion-id",
                "delete-one",
            ],
            serde_json::json!({"incarnation":"inc-one","deletion_id":"delete-one"}),
        ),
    ] {
        let temp = tempdir().unwrap();
        let api = lifecycle_fixture(vec![IntentReply::json(
            202,
            lifecycle_envelope(kind, "proposed", "requested"),
        )]);
        write_managed_target(temp.path(), &api.origin);
        let before = fs::read(managed_home(temp.path()).join("config.yaml")).unwrap();
        let output = output_success(
            managed_fixture_command(temp.path(), &api.origin, "managed-test")
                .current_dir(temp.path())
                .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
                .env("OMNIGRAPH_CONTROL_API", &api.origin)
                .arg("cluster")
                .args(args)
                .args(["--no-wait", "--idempotency-key", "exact-intent", "--json"]),
        );
        assert_eq!(parse_stdout_json(&output)["data"]["kind"], kind);
        assert_control_request(
            &api.workflow_requests()[0],
            "POST",
            &format!("/v1/clusters/managed-test:{kind}"),
            expected,
            Some("exact-intent"),
        );
        assert_eq!(
            fs::read(managed_home(temp.path()).join("config.yaml")).unwrap(),
            before
        );
        api.assert_complete();
        assert_no_core_effects(temp.path());
    }
}

#[test]
#[ignore = "nightly: two real 2 s managed poll intervals"]
fn managed_lifecycle_wait_reports_tombstone_and_checks_every_poll_identity() {
    let temp = tempdir().unwrap();
    let tombstone = lifecycle_envelope("delete", "running", "tombstoned");
    let api = lifecycle_fixture(vec![
        IntentReply::json(202, lifecycle_envelope("delete", "proposed", "requested")),
        IntentReply::json(200, tombstone.clone()),
    ]);
    write_managed_target(temp.path(), &api.origin);
    let output = output_success(
        managed_fixture_command(temp.path(), &api.origin, "managed-test")
            .current_dir(temp.path())
            .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
            .env("OMNIGRAPH_CONTROL_API", &api.origin)
            .args([
                "cluster",
                "delete",
                "--incarnation",
                "inc-one",
                "--timeout",
                "10",
                "--json",
            ]),
    );
    assert_eq!(parse_stdout_json(&output), tombstone);
    assert!(String::from_utf8_lossy(&output.stderr).contains("undo deadline:"));
    assert_eq!(api.workflow_requests()[0].body["retention_seconds"], 86400);
    assert_eq!(
        api.workflow_requests()[1].path,
        "/v1/operations/operation-one"
    );
    api.assert_complete();

    let temp = tempdir().unwrap();
    let mut mismatched = lifecycle_envelope("create", "converged", "ready");
    mismatched["data"]["incarnation"] = serde_json::json!("other-incarnation");
    mismatched["meta"]["incarnation"] = serde_json::json!("other-incarnation");
    let api = lifecycle_fixture(vec![
        IntentReply::json(
            200,
            lifecycle_envelope("create", "running", "bootstrapping"),
        ),
        IntentReply::json(200, mismatched),
    ]);
    let output = managed_fixture_command(temp.path(), &api.origin, "managed-test")
        .current_dir(temp.path())
        .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
        .env("OMNIGRAPH_CONTROL_API", &api.origin)
        .args([
            "cluster",
            "operation",
            "operation-one",
            "--wait",
            "--timeout",
            "10",
            "--json",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(parse_stdout_json(&output)["type"], "target_mismatch");
    assert!(!temp.path().join(".omnigraph/context").exists());
    api.assert_complete();
}

#[test]
#[ignore = "nightly: a real 1 s --timeout slept to the deadline"]
fn managed_lifecycle_bad_acceptance_and_deadline_keep_recovery_identity() {
    let temp = tempdir().unwrap();
    let mut bad = lifecycle_envelope("create", "proposed", "provisioning");
    bad["meta"]["cluster_id"] = serde_json::json!("foreign");
    let api = lifecycle_fixture(vec![IntentReply::json(202, bad)]);
    let output = managed_fixture_command(temp.path(), &api.origin, "managed-account")
        .current_dir(temp.path())
        .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
        .env("OMNIGRAPH_CONTROL_API", &api.origin)
        .args(["cluster", "create", "new", "--no-wait", "--json"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(lifecycle_record(temp.path(), &api.origin, "pending-lifecycle.json").exists());
    assert!(!temp.path().join(".omnigraph/context").exists());
    api.assert_complete();

    let temp = tempdir().unwrap();
    let accepted = lifecycle_envelope("create", "proposed", "provisioning");
    let api = lifecycle_fixture(vec![IntentReply::json(202, accepted.clone())]);
    let output = managed_fixture_command(temp.path(), &api.origin, "managed-account")
        .current_dir(temp.path())
        .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
        .env("OMNIGRAPH_CONTROL_API", &api.origin)
        .args(["cluster", "create", "new", "--timeout", "1", "--json"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(5));
    assert_eq!(parse_stdout_json(&output), accepted);
    assert!(lifecycle_record(temp.path(), &api.origin, "accepted-lifecycle.json").exists());
    assert_eq!(api.workflow_requests().len(), 1);
    api.assert_complete();
    assert_no_core_effects(temp.path());
}

#[test]
fn managed_cluster_list_uses_selected_account_without_folder_routing() {
    let temp = tempdir().unwrap();
    let response =
        serde_json::json!({"data":{"organization_id":"account-one","clusters":[]},"meta":{}});
    let api = lifecycle_fixture(vec![IntentReply::json(200, response.clone())]);
    write_managed_target(temp.path(), &api.origin);
    let output =
        output_success(managed_account_cli(temp.path(), &api.origin).args(["list", "--json"]));
    assert_eq!(parse_stdout_json(&output), response);
    assert_control_request(
        &api.workflow_requests()[0],
        "GET",
        "/v1/clusters",
        Value::Null,
        None,
    );
    assert!(!temp.path().join(".omnigraph").exists());
    api.assert_complete();
}

fn member_directory() -> Value {
    serde_json::json!({
        "data": {"cluster_id":"managed-test","members":[
            {"user_id":"user_one","principal_id":"principal-one","name":"Avery Chen","email":"avery@example.com","admitted":true,"control_grants":["source_read","status"]},
            {"user_id":"user_two","principal_id":"principal-two","name":null,"email":null,"admitted":false,"control_grants":["status"]}
        ]},
        "meta":{"cluster_id":"managed-test","incarnation":"inc-one","provenance":"service_db"}
    })
}

#[test]
fn managed_cluster_members_lists_exact_enrollment_as_json_and_readable_text() {
    for json in [true, false] {
        let temp = tempdir().unwrap();
        let response = member_directory();
        let api = lifecycle_fixture(vec![IntentReply::json(200, response.clone())]);
        write_managed_target(temp.path(), &api.origin);
        let before = fs::read(managed_home(temp.path()).join("config.yaml")).unwrap();
        // Exercise the same explicit target through both nested command levels.
        let mut command = managed_fixture_command(temp.path(), &api.origin, "managed-test");
        command
            .current_dir(temp.path())
            .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
            .env("OMNIGRAPH_CONTROL_API", &api.origin)
            .args(["cluster", "members", "list"]);
        if json {
            command.arg("--json");
        }
        let output = output_success(&mut command);
        if json {
            assert_eq!(parse_stdout_json(&output), response);
        } else {
            let text = String::from_utf8(output.stdout).unwrap();
            for expected in [
                "NAME",
                "EMAIL",
                "PRINCIPAL",
                "ADMITTED",
                "CONTROL GRANTS",
                "Avery Chen",
                "avery@example.com",
                "principal-one",
                "principal-two",
                "source_read, status",
                "Graph permissions are evaluated by Cedar separately.",
            ] {
                assert!(text.contains(expected), "missing {expected}: {text}");
            }
            assert!(!text.contains("\"data\""));
        }
        assert_control_request(
            &api.workflow_requests()[0],
            "GET",
            "/v1/clusters/managed-test/members",
            Value::Null,
            None,
        );
        assert_eq!(
            fs::read(managed_home(temp.path()).join("config.yaml")).unwrap(),
            before
        );
        assert_no_core_effects(temp.path());
        api.assert_complete();
    }
    let temp = tempdir().unwrap();
    let mut empty = member_directory();
    empty["data"]["members"] = serde_json::json!([]);
    let api = lifecycle_fixture(vec![IntentReply::json(200, empty)]);
    write_managed_target(temp.path(), &api.origin);
    let output = output_success(managed_cli(temp.path(), &api.origin).args(["members", "list"]));
    assert!(String::from_utf8_lossy(&output.stdout).contains("No explicitly enrolled members."));
    api.assert_complete();
}

#[test]
fn managed_cluster_members_refuses_wrong_identity_malformed_results_and_denials() {
    let mut cases = vec![];
    for (pointer, value, kind, exit) in [
        (
            "/meta/cluster_id",
            serde_json::json!("foreign"),
            "context_mismatch",
            2,
        ),
        (
            "/data/cluster_id",
            serde_json::json!("foreign"),
            "context_mismatch",
            2,
        ),
        ("/meta/incarnation", Value::Null, "target_mismatch", 2),
        (
            "/meta/incarnation",
            serde_json::json!("foreign-incarnation"),
            "target_mismatch",
            2,
        ),
        (
            "/data/members/0/admitted",
            serde_json::json!("yes"),
            "api_response_invalid",
            1,
        ),
        (
            "/data/members/0/name",
            serde_json::json!("bad\u{001b}[2J"),
            "api_response_invalid",
            1,
        ),
        (
            "/data/members/0/control_grants",
            serde_json::json!(["status", "status"]),
            "api_response_invalid",
            1,
        ),
        (
            "/data/members/1/control_grants",
            serde_json::json!([]),
            "api_response_invalid",
            1,
        ),
        (
            "/data/members/1/principal_id",
            serde_json::json!("principal-one"),
            "api_response_invalid",
            1,
        ),
    ] {
        let mut response = member_directory();
        *response.pointer_mut(pointer).unwrap() = value;
        cases.push((200, response, kind, exit));
    }
    for (status, kind) in [
        (401, "unauthorized"),
        (403, "scope_missing"),
        (404, "cluster_not_found"),
    ] {
        cases.push((
            status,
            serde_json::json!({"type":kind,"detail":"not permitted"}),
            kind,
            2,
        ));
    }
    for (status, response, kind, exit) in cases {
        let temp = tempdir().unwrap();
        let api = lifecycle_fixture(vec![IntentReply::json(status, response)]);
        write_managed_target(temp.path(), &api.origin);
        let output = managed_cli(temp.path(), &api.origin)
            .args(["members", "list", "--json"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(exit), "{output:?}");
        let problem = parse_stdout_json(&output);
        assert_eq!(problem["type"], kind);
        assert!(
            problem.get("data").is_none(),
            "member data leaked in failure"
        );
        assert_eq!(api.workflow_requests().len(), 1);
        assert_no_core_effects(temp.path());
        api.assert_complete();
    }
}

#[test]
fn managed_cluster_members_requires_cluster_target_and_rejects_duplicate_selectors() {
    let temp = tempdir().unwrap();
    let api = lifecycle_fixture(vec![]);
    write_managed_target(temp.path(), &api.origin);
    let output = output_failure(
        managed_account_cli(temp.path(), &api.origin).args(["members", "list", "--json"]),
    );
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("cluster does not accept --profile"),
        "{output:?}"
    );
    for args in [
        vec!["--direct"],
        vec!["--target", "https://other.example"],
        vec!["--server", "https://other.example"],
    ] {
        let output = output_failure(
            managed_cli(temp.path(), &api.origin)
                .args(["members", "list", "--json"])
                .args(args),
        );
        assert_eq!(output.status.code(), Some(64));
    }
    assert!(api.requests().is_empty());
    assert_no_core_effects(temp.path());
    api.assert_complete();
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn source_export(files: Value) -> Value {
    serde_json::json!({"data":{"cluster_id":"managed-test","revision":MANAGED_REVISION,"files":files},"meta":{"cluster_id":"managed-test","incarnation":"inc-one","provenance":"service_db"}})
}

#[test]
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn managed_cluster_pull_downloads_exact_source_and_context_without_printing_contents() {
    use std::os::unix::fs::PermissionsExt;
    for explicit_revision in [false, true] {
        let temp = tempdir().unwrap();
        let files = serde_json::json!({
            "cluster.yaml":"version: 1\nstorage: s3://tenant/root\ngraphs: {}\npolicies:\n  access:\n    file: policies/access.yaml\n",
            "policies/access.yaml":"# og_fixture_control must survive byte-exact download\npolicies: []\n",
        });
        let api = lifecycle_fixture(vec![IntentReply::json(200, source_export(files.clone()))]);
        write_managed_target(temp.path(), &api.origin);
        let mut command = managed_cli(temp.path(), &api.origin);
        command.args(["pull", "--out", "checkout", "--json"]);
        if explicit_revision {
            command.args(["--rev", MANAGED_REVISION]);
        }
        let output = output_success(&mut command);
        let body = parse_stdout_json(&output);
        assert_eq!(body["data"]["revision"], MANAGED_REVISION);
        assert_eq!(body["data"]["file_count"], 2);
        assert!(body["data"].get("files").is_none());
        assert!(!String::from_utf8_lossy(&output.stdout).contains("og_fixture_control"));
        for (path, content) in files.as_object().unwrap() {
            let file = temp.path().join("checkout").join(path);
            assert_eq!(
                fs::read_to_string(&file).unwrap(),
                content.as_str().unwrap()
            );
            assert_eq!(
                fs::metadata(file).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        assert!(!temp.path().join("checkout/.omnigraph").exists());
        assert_eq!(
            fs::metadata(temp.path().join("checkout"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        let expected = format!(
            "/v1/clusters/managed-test/config/export{}",
            if explicit_revision {
                format!("?revision={MANAGED_REVISION}")
            } else {
                String::new()
            }
        );
        assert_control_request(
            &api.workflow_requests()[0],
            "GET",
            &expected,
            Value::Null,
            None,
        );
        api.assert_complete();
        assert_no_core_effects(temp.path());
    }
}

#[test]
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn managed_cluster_pull_refuses_foreign_invalid_and_unsafe_exports_without_partial_output() {
    let normal = serde_json::json!({"cluster.yaml":"version: 1\ngraphs: {}\n"});
    let mut foreign = source_export(normal.clone());
    foreign["meta"]["cluster_id"] = serde_json::json!("another-cluster");
    let mut wrong_revision = source_export(normal.clone());
    wrong_revision["data"]["revision"] = serde_json::json!("b".repeat(40));
    let mut cases = vec![foreign, wrong_revision];
    for bad_path in [
        "../outside",
        "/absolute",
        "a/../../outside",
        "a//b",
        "./schema.pg",
        "a\\b",
        "c:drive",
        ".omnigraph/context",
        "a/.git/config",
        "__cluster/state.json",
    ] {
        let mut files = normal.clone();
        files[bad_path] = serde_json::json!("never write");
        cases.push(source_export(files));
    }
    for files in [
        serde_json::json!({}),
        serde_json::json!({"only.pg":"node Test {}"}),
        serde_json::json!({"cluster.yaml":""}),
        serde_json::json!({"cluster.yaml":17}),
        serde_json::json!({"cluster.yaml":"version: 1", "a":"file", "a/b":"collision"}),
        serde_json::json!({"cluster.yaml":"version: 1", "Schema.pg":"a", "schema.pg":"b"}),
        serde_json::json!({"cluster.yaml":"version: 1", "large.pg":"x".repeat(1024 * 1024 + 1)}),
    ] {
        cases.push(source_export(files));
    }
    for response in cases {
        let temp = tempdir().unwrap();
        let api = lifecycle_fixture(vec![IntentReply::json(200, response)]);
        write_managed_target(temp.path(), &api.origin);
        let output = managed_cli(temp.path(), &api.origin)
            .args([
                "pull",
                "--out",
                "checkout",
                "--rev",
                MANAGED_REVISION,
                "--json",
            ])
            .output()
            .unwrap();
        assert!(!output.status.success(), "{:?}", output);
        assert!(!temp.path().join("checkout").exists());
        assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 1);
        api.assert_complete();
    }
}

#[test]
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn managed_cluster_pull_preserves_existing_destinations_before_http() {
    use std::os::unix::fs::symlink;
    let temp = tempdir().unwrap();
    let api = lifecycle_fixture(vec![]);
    write_managed_target(temp.path(), &api.origin);
    fs::create_dir(temp.path().join("empty")).unwrap();
    fs::write(temp.path().join("edited"), "keep my work").unwrap();
    symlink("missing", temp.path().join("link")).unwrap();
    for out in ["empty", "empty/.", "edited", "link", ".", "missing/child"] {
        let output = managed_cli(temp.path(), &api.origin)
            .args(["pull", "--out", out, "--json"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
    }
    assert_eq!(
        fs::read_to_string(temp.path().join("edited")).unwrap(),
        "keep my work"
    );
    assert_eq!(fs::read_dir(temp.path().join("empty")).unwrap().count(), 0);
    assert!(
        fs::symlink_metadata(temp.path().join("link"))
            .unwrap()
            .is_symlink()
    );
    assert!(api.requests().is_empty());
    api.assert_complete();
}

#[test]
#[cfg(unix)]
fn managed_lifecycle_push_sends_only_complete_referenced_files() {
    let temp = tempdir().unwrap();
    let response = serde_json::json!({"data":{"cluster_id":"managed-test","revision":"a".repeat(40)},"meta":{"cluster_id":"managed-test","incarnation":"inc-one"}});
    let api = lifecycle_fixture(vec![IntentReply::json(200, response.clone())]);
    write_managed_target(temp.path(), &api.origin);
    fs::create_dir(temp.path().join("queries")).unwrap();
    let files = [
        (
            "cluster.yaml",
            "version: 1\ngraphs:\n  sample:\n    schema: ./schema.pg\n    queries: queries/\npolicies:\n  access:\n    file: policy.yaml\n",
        ),
        ("schema.pg", "node Person { name: String @key }\n"),
        (
            "queries/names.gq",
            "query names() { match { $p: Person } return { $p.name } }\n",
        ),
        ("policy.yaml", "policies: []\n"),
    ];
    for (path, text) in files {
        fs::write(temp.path().join(path), text).unwrap();
    }
    fs::write(temp.path().join("private.key"), "NEVER_UPLOAD_ME").unwrap();
    fs::write(temp.path().join("queries/ignored.txt"), "NEVER_UPLOAD_ME").unwrap();
    let output = output_success(
        managed_fixture_command(temp.path(), &api.origin, "managed-test")
            .current_dir(temp.path())
            .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
            .env("OMNIGRAPH_CONTROL_API", &api.origin)
            .args([
                "cluster",
                "push",
                "--expected-revision",
                &"b".repeat(40),
                "--message",
                "prepare",
                "--json",
            ]),
    );
    assert_eq!(parse_stdout_json(&output), response);
    let requests = api.workflow_requests();
    assert_eq!(requests[0].body["files"].as_object().unwrap().len(), 4);
    for (path, text) in files {
        assert_eq!(requests[0].body["files"][path], text);
    }
    assert!(!requests[0].body.to_string().contains("NEVER_UPLOAD_ME"));
    assert_eq!(requests[0].path, "/v1/clusters/managed-test/config");
    assert!(!requests[0].headers.contains_key("idempotency-key"));
    api.assert_complete();
    assert_no_core_effects(temp.path());
}

#[test]
fn managed_lifecycle_push_refuses_unsafe_paths_and_oversized_files_before_http() {
    let temp = tempdir().unwrap();
    let api = lifecycle_fixture(vec![]);
    write_managed_target(temp.path(), &api.origin);
    fs::write(
        temp.path().join("large.pg"),
        vec![b'x'; 2 * 1024 * 1024 + 1],
    )
    .unwrap();
    fs::write(temp.path().join("binary.pg"), [0xff, 0xfe]).unwrap();
    for path in [
        "../escape.pg",
        "/absolute.pg",
        ".omnigraph/context",
        "large.pg",
        "binary.pg",
        "missing.pg",
    ] {
        fs::write(
            temp.path().join("cluster.yaml"),
            format!("version: 1\ngraphs:\n  sample:\n    schema: {path}\n"),
        )
        .unwrap();
        let output = managed_fixture_command(temp.path(), &api.origin, "managed-test")
            .current_dir(temp.path())
            .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
            .env("OMNIGRAPH_CONTROL_API", &api.origin)
            .args([
                "cluster",
                "push",
                "--expected-revision",
                &"b".repeat(40),
                "--message",
                "prepare",
                "--json",
            ])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{path}: {output:?}");
        assert_eq!(parse_stdout_json(&output)["type"], "config_capture_failed");
    }
    assert!(api.workflow_requests().is_empty());
    assert_no_core_effects(temp.path());
}

#[test]
fn managed_lifecycle_local_lock_and_direct_flags_refuse_without_submission() {
    let temp = tempdir().unwrap();
    let api = lifecycle_fixture(vec![]);
    write_managed_target(temp.path(), &api.origin);
    fs::create_dir_all(
        lifecycle_record(temp.path(), &api.origin, "lifecycle.lock")
            .parent()
            .unwrap(),
    )
    .unwrap();
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lifecycle_record(temp.path(), &api.origin, "lifecycle.lock"))
        .unwrap();
    lock.try_lock().unwrap();
    let output = managed_fixture_command(temp.path(), &api.origin, "managed-account")
        .current_dir(temp.path())
        .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
        .env("OMNIGRAPH_CONTROL_API", &api.origin)
        .args(["cluster", "create", "locked", "--no-wait", "--json"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(
        parse_stdout_json(&output)["type"],
        "lifecycle_submission_busy"
    );
    assert!(!lifecycle_record(temp.path(), &api.origin, "pending-lifecycle.json").exists());
    drop(lock);
    for args in [
        vec!["cluster", "create", "direct", "--direct"],
        vec!["cluster", "delete", "--incarnation", "inc-one", "--direct"],
        vec![
            "cluster",
            "undo-delete",
            "--incarnation",
            "inc-one",
            "--deletion-id",
            "op",
            "--direct",
        ],
        vec!["cluster", "operation", "op", "--direct"],
    ] {
        let output = managed_fixture_command(temp.path(), &api.origin, "managed-test")
            .current_dir(temp.path())
            .args(args)
            .arg("--json")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(64));
        assert!(String::from_utf8_lossy(&output.stderr).contains("unexpected argument"));
    }
    assert!(api.workflow_requests().is_empty());
    assert_no_core_effects(temp.path());
}

#[cfg(unix)]
#[test]
fn managed_lifecycle_capture_and_retry_records_refuse_symlinks_without_reading_targets() {
    let temp = tempdir().unwrap();
    let outside = tempdir().unwrap();
    let api = lifecycle_fixture(vec![]);
    write_managed_target(temp.path(), &api.origin);
    fs::write(outside.path().join("private.pg"), "DO_NOT_SEND").unwrap();
    std::os::unix::fs::symlink(outside.path(), temp.path().join("linked")).unwrap();
    fs::write(
        temp.path().join("cluster.yaml"),
        "version: 1\ngraphs:\n  sample:\n    schema: linked/private.pg\n",
    )
    .unwrap();
    let output = managed_fixture_command(temp.path(), &api.origin, "managed-test")
        .current_dir(temp.path())
        .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
        .env("OMNIGRAPH_CONTROL_API", &api.origin)
        .args([
            "cluster",
            "push",
            "--expected-revision",
            &"b".repeat(40),
            "--message",
            "prepare",
            "--json",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(parse_stdout_json(&output)["type"], "config_capture_failed");
    fs::create_dir_all(
        lifecycle_record(temp.path(), &api.origin, "pending-lifecycle.json")
            .parent()
            .unwrap(),
    )
    .unwrap();
    std::os::unix::fs::symlink(
        outside.path().join("private.pg"),
        lifecycle_record(temp.path(), &api.origin, "pending-lifecycle.json"),
    )
    .unwrap();
    let output = managed_fixture_command(temp.path(), &api.origin, "managed-test")
        .current_dir(temp.path())
        .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
        .env("OMNIGRAPH_CONTROL_API", &api.origin)
        .args([
            "cluster",
            "delete",
            "--incarnation",
            "inc-one",
            "--no-wait",
            "--json",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        fs::read_to_string(outside.path().join("private.pg")).unwrap(),
        "DO_NOT_SEND"
    );
    assert!(!String::from_utf8_lossy(&output.stdout).contains("DO_NOT_SEND"));
    assert!(api.workflow_requests().is_empty());
}

#[test]
fn managed_data_process_refuses_missing_graph_and_actor_override_before_keychain() {
    let temp = tempdir().unwrap();
    let api = IntentApiFixture::new(vec![]);
    write_managed_target(temp.path(), &api.origin);
    for (args, code, message) in [
        (
            vec!["query", "q", "--json"],
            "target_incompatible",
            "graph_required",
        ),
        (
            vec![
                "mutate",
                "m",
                "--graph",
                "knowledge",
                "--as",
                "forged",
                "--json",
            ],
            "target_incompatible",
            "--as is not available for a managed target",
        ),
        (
            vec![
                "cluster",
                "token",
                "--clear",
                "--graph",
                "knowledge",
                "--json",
            ],
            "argument_conflict",
            "cluster does not accept --graph",
        ),
    ] {
        let output = managed_fixture_command(temp.path(), &api.origin, "managed-test")
            .current_dir(temp.path())
            .env_remove("OMNIGRAPH_PROFILE")
            .args(&args)
            .output()
            .unwrap();
        if code == "argument_conflict" {
            assert_eq!(output.status.code(), Some(1), "{args:?}: {output:?}");
            assert!(output.stdout.is_empty());
            assert!(
                String::from_utf8_lossy(&output.stderr).contains(message),
                "{output:?}"
            );
            assert_no_core_effects(temp.path());
            continue;
        }
        let exit = if args[0] == "cluster" { 2 } else { 1 };
        assert_eq!(output.status.code(), Some(exit), "{args:?}: {output:?}");
        let problem = parse_stdout_json(&output);
        assert_eq!(problem["code"], code, "{args:?}: {problem}");
        assert!(
            problem["error"].as_str().unwrap().contains(message),
            "{args:?}: {problem}"
        );
        assert_no_core_effects(temp.path());
    }
    assert!(api.requests().is_empty());
    assert_no_core_effects(temp.path());
}

#[test]
fn managed_data_named_server_ignores_folder_context() {
    let temp = tempdir().unwrap();
    let api = IntentApiFixture::new(vec![]);
    write_managed_target(temp.path(), &api.origin);
    fs::create_dir_all(temp.path().join(".omnigraph")).unwrap();
    fs::write(temp.path().join(".omnigraph/context"), "malformed").unwrap();
    let data = IntentApiFixture::graph(vec![IntentReply::json(
        200,
        serde_json::json!({
            "query_name":"q", "target":{"branch":"main"}, "row_count":1,
            "columns":["value"], "rows":[{"value":42}], "graph_commit_id":"head-a"
        }),
    )]);
    let operator = KeyedServer::new(&data.origin);
    let output = output_success(
        cli()
            .current_dir(temp.path())
            .envs(operator.env("explicit-legacy-token"))
            .env("OMNIGRAPH_CONTROL_TOKEN", "never-data")
            .env("OMNIGRAPH_CONTROL_API", &api.origin)
            .args(["query", "q", "--graph", "knowledge", "--target"])
            .arg(KEYED_SERVER_NAME)
            .arg("--json"),
    );
    assert_eq!(
        parse_stdout_json(&output)["rows"],
        serde_json::json!([{"value":42}])
    );
    let requests = data.workflow_requests();
    assert_eq!(requests[0].path, "/graphs/knowledge/queries/q");
    assert_eq!(
        requests[0].headers["authorization"],
        "Bearer explicit-legacy-token"
    );
    assert!(api.requests().is_empty());
    data.assert_complete();
    assert_no_core_effects(temp.path());
}

#[test]
fn managed_data_issue_633_explicit_targets_ignore_folder_context() {
    let ca = tls_gateway::ca_path().display();
    for malformed in [false, true] {
        for selector in ["--target"] {
            for verb in ["query", "mutate", "load"] {
                let temp = tempdir().unwrap();
                let api = IntentApiFixture::new(vec![]);
                write_managed_target(temp.path(), &api.origin);
                fs::create_dir_all(temp.path().join(".omnigraph")).unwrap();
                fs::write(
                    temp.path().join(".omnigraph/context"),
                    "version: 2\ncluster: ignored\n",
                )
                .unwrap();
                if malformed {
                    fs::write(temp.path().join(".omnigraph/context"), "malformed").unwrap();
                }
                let reply = if verb == "query" {
                    serde_json::json!({
                        "query_name":"q", "target":{"branch":"main"}, "row_count":1,
                        "columns":["value"], "rows":[{"value":42}], "graph_commit_id":"head-a"
                    })
                } else if verb == "mutate" {
                    serde_json::json!({
                        "branch":"main", "query_name":"q", "affected_nodes":1,
                        "affected_edges":0, "actor_id":"legacy-actor", "commit":null
                    })
                } else {
                    serde_json::json!({
                        "branch":"main", "base_branch":null, "branch_created":false,
                        "mode":"append", "nodes":[{"name":"Person", "entities_loaded":1}],
                        "edges":[], "total_entities":1, "actor_id":"legacy-actor", "commit":null,
                        "embedding_generation":null
                    })
                };
                let data = IntentApiFixture::graph(vec![IntentReply::json(200, reply)]);
                let home = temp.path().join("operator");
                fs::create_dir(&home).unwrap();
                fs::write(
                    home.join("config.yaml"),
                    format!(
                        "version: 1\ntargets:\n  staging:\n    kind: server\n    transport:\n      tls:\n        ca: {{mode: replace, file: '{ca}'}}\n    endpoint: {}\n    auth:\n      provider: token\n      source:\n        env: OMNIGRAPH_TOKEN_STAGING\n    default_graph: knowledge\n",
                        data.origin
                    ),
                )
                .unwrap();
                let mut command = cli();
                command
                    .current_dir(temp.path())
                    .env("OMNIGRAPH_HOME", &home)
                    .env("OMNIGRAPH_PROFILE", "unused-unknown-profile")
                    .env("OMNIGRAPH_TOKEN_STAGING", "explicit-legacy-token")
                    .env("OMNIGRAPH_BEARER_TOKEN", "unused-legacy-fallback")
                    .env("OMNIGRAPH_CONTROL_API", &api.origin)
                    .env("OMNIGRAPH_CONTROL_TOKEN", "never-data")
                    .args([verb, selector, "staging", "--json"])
                    .timeout(std::time::Duration::from_secs(15));
                if verb == "load" {
                    fs::write(temp.path().join("batch.jsonl"), "{}\n").unwrap();
                    command.args(["--data", "batch.jsonl", "--mode", "append"]);
                } else {
                    command.arg("q");
                }
                if selector == "--target" {
                    command.args(["--graph", "knowledge"]);
                }
                let output = output_success(&mut command);
                let payload = parse_stdout_json(&output);
                if verb == "query" {
                    assert_eq!(payload["rows"], serde_json::json!([{"value":42}]));
                } else if verb == "mutate" {
                    assert_eq!(payload["affected_nodes"], 1);
                } else {
                    assert_eq!(payload["total_entities"], 1);
                }
                let requests = data.workflow_requests();
                assert_eq!(requests.len(), 1);
                assert_eq!(
                    requests[0].path,
                    if verb == "load" {
                        "/graphs/knowledge/load/ndjson?branch=main&mode=append"
                    } else {
                        "/graphs/knowledge/queries/q"
                    }
                );
                assert_eq!(
                    requests[0].headers["authorization"],
                    "Bearer explicit-legacy-token"
                );
                assert!(api.requests().is_empty());
                data.assert_complete();
                assert_no_core_effects(temp.path());
            }
        }
    }
}

#[test]
fn managed_status_and_history_verify_selected_target_identity() {
    let temp = tempdir().unwrap();
    let status = serde_json::json!({"data":{"source":{"revision":MANAGED_REVISION},"requested":null,"observed":{"state":"observation_blocked"}},"meta":{"cluster_id":"managed-test","incarnation":"inc-one","provenance":"service_db"}});
    let delivery = managed_complete(false, false, "archived");
    let history = serde_json::json!({"data":{"deployments":[delivery["data"].clone()]},"meta":{"cluster_id":"managed-test","incarnation":"inc-one","provenance":"service_db"}});
    let mut reads = Vec::new();
    for (args, path, response) in [
        (vec!["status"], "/v1/clusters/managed-test/status", status),
        (
            vec!["status", "delivery-one"],
            "/v1/clusters/managed-test/deployments/delivery-one",
            delivery,
        ),
        (
            vec!["history"],
            "/v1/clusters/managed-test/history?limit=100",
            history.clone(),
        ),
        (
            vec![
                "history",
                "--limit",
                "7",
                "--since",
                "2026-09-29T12:30:00+02:00",
            ],
            "/v1/clusters/managed-test/history?limit=7&since=2026-09-29T12%3A30%3A00%2B02%3A00",
            history,
        ),
    ] {
        reads.push((args.clone(), path, response.clone(), false));
        let mut foreign = response.clone();
        foreign["meta"]["cluster_id"] = "another-cluster".into();
        reads.push((args.clone(), path, foreign, true));
        if args == ["status", "delivery-one"] {
            let mut foreign = response;
            foreign["data"]["deployment_id"] = "another-delivery".into();
            reads.push((args, path, foreign, true));
        }
    }
    let replies = reads
        .iter()
        .map(|(_, _, body, _)| IntentReply::json(200, body.clone()))
        .collect();
    let api = lifecycle_fixture(replies);
    write_managed_target(temp.path(), &api.origin);
    let config_path = managed_home(temp.path()).join("config.yaml");
    let config = fs::read_to_string(&config_path).unwrap();
    fs::write(
        &config_path,
        format!(
            "{}\ndefaults:\n  profile: other\n",
            config.replace(
                "profiles:\n",
                "profiles:\n  other:\n    api: https://other.example.test\n    organization_id: account-two\n    auth: {provider: token, source: {env: CLI_TEST_OTHER_TOKEN}}\n",
            )
        ),
    )
    .unwrap();
    for (index, (args, path, response, refused)) in reads.into_iter().enumerate() {
        let output = managed_cli(temp.path(), &api.origin)
            .env_remove("CLI_TEST_OTHER_TOKEN")
            .args(args)
            .arg("--json")
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(if refused { 2 } else { 0 }),
            "{output:?}"
        );
        if refused {
            assert_eq!(parse_stdout_json(&output)["type"], "context_mismatch");
        } else {
            assert_eq!(parse_stdout_json(&output), response);
        }
        assert_eq!(api.workflow_requests().len(), index + 1);
        assert_control_request(
            &api.workflow_requests()[index],
            "GET",
            path,
            Value::Null,
            None,
        );
        assert_no_core_effects(temp.path());
    }
    let invalid = managed_cli(temp.path(), &api.origin)
        .args(["history", "--since", "not-a-time", "--json"])
        .output()
        .unwrap();
    assert_eq!(invalid.status.code(), Some(2));
    assert_eq!(parse_stdout_json(&invalid)["type"], "since_invalid");
    api.assert_complete();
}

#[test]
fn managed_plan_and_apply_submit_exact_native_intent_without_waiting() {
    for (arguments, path, expected, body) in [
        (
            vec!["plan", "--rev", MANAGED_REVISION],
            "/v1/clusters/managed-test/plans",
            serde_json::json!({"revision":MANAGED_REVISION}),
            managed_preview("ready"),
        ),
        (
            vec!["apply", "--plan", "saved-plan"],
            "/v1/clusters/managed-test/deployments",
            serde_json::json!({"preview_id":"saved-plan"}),
            managed_delivery("queued"),
        ),
    ] {
        let temp = tempdir().unwrap();
        write_cluster_config_fixture(temp.path());
        let api = lifecycle_fixture(vec![IntentReply::json(202, body.clone())]);
        write_managed_target(temp.path(), &api.origin);
        let output = output_success(managed_cli(temp.path(), &api.origin).args(arguments).args([
            "--no-wait",
            "--idempotency-key",
            "exact-key",
            "--json",
        ]));
        assert_eq!(parse_stdout_json(&output), body);
        assert!(String::from_utf8_lossy(&output.stderr).contains("exact-key"));
        assert!(!String::from_utf8_lossy(&output.stderr).contains("og_fixture_control"));
        assert_control_request(
            &api.workflow_requests()[0],
            "POST",
            path,
            expected,
            Some("exact-key"),
        );
        api.assert_complete();
        assert_no_core_effects(temp.path());
    }
}

#[test]
fn managed_plan_omitted_revision_captures_public_source_head_once() {
    for state in ["ready", "capturing", "failed", "expired"] {
        let temp = tempdir().unwrap();
        let source = serde_json::json!({"data":{"revision":MANAGED_REVISION},"meta":{"cluster_id":"managed-test","incarnation":"inc-one"}});
        let expected = managed_preview(state);
        let api = lifecycle_fixture(vec![
            IntentReply::json(200, source),
            IntentReply::json(200, expected.clone()),
        ]);
        write_managed_target(temp.path(), &api.origin);
        let output = managed_cli(temp.path(), &api.origin)
            .args(["plan", "--idempotency-key", "snapshot", "--json"])
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(match state {
                "ready" => 0,
                "capturing" => 5,
                _ => 1,
            })
        );
        assert_eq!(parse_stdout_json(&output), expected);
        assert_control_request(
            &api.workflow_requests()[0],
            "GET",
            "/v1/clusters/managed-test/config",
            Value::Null,
            None,
        );
        assert_control_request(
            &api.workflow_requests()[1],
            "POST",
            "/v1/clusters/managed-test/plans",
            serde_json::json!({"revision":MANAGED_REVISION}),
            Some("snapshot"),
        );
        api.assert_complete();
        assert_no_core_effects(temp.path());
    }
}

#[test]
#[ignore = "nightly: real 2 s delivery polling and a 1 s wait deadline"]
fn managed_apply_polls_original_delivery_without_resubmission_or_cancellation() {
    for timeout in [false, true] {
        let temp = tempdir().unwrap();
        let queued = managed_delivery("queued");
        let complete = managed_complete(true, true, "archived");
        let replies = if timeout {
            vec![IntentReply::json(202, queued.clone())]
        } else {
            vec![
                IntentReply::json(202, queued.clone()),
                IntentReply::json(200, complete.clone()),
            ]
        };
        let api = lifecycle_fixture(replies);
        write_managed_target(temp.path(), &api.origin);
        let output = managed_cli(temp.path(), &api.origin)
            .args([
                "apply",
                "--plan",
                "saved-plan",
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
            "{output:?}"
        );
        assert_eq!(
            parse_stdout_json(&output),
            if timeout { queued } else { complete }
        );
        if timeout {
            assert!(
                String::from_utf8_lossy(&output.stderr)
                    .contains("cluster status delivery-one --target NAME")
            );
        }
        assert_control_request(
            &api.workflow_requests()[0],
            "POST",
            "/v1/clusters/managed-test/deployments",
            serde_json::json!({"preview_id":"saved-plan"}),
            Some("poll-key"),
        );
        if !timeout {
            assert_control_request(
                &api.workflow_requests()[1],
                "GET",
                "/v1/clusters/managed-test/deployments/delivery-one",
                Value::Null,
                None,
            );
        }
        api.assert_complete();
        assert_no_core_effects(temp.path());
    }
}

#[test]
#[ignore = "nightly: real 1 s native-outcome wait deadlines"]
fn managed_native_outcomes_preserve_activation_archive_and_uncertainty() {
    let mut refused = managed_delivery("dispatched");
    refused["data"]["native_result_status"] = serde_json::json!("refused");
    let mut blocked = managed_delivery("dispatch_unknown");
    blocked["data"]["observation"] =
        serde_json::json!({"state":"observation_blocked","reason":"native_authorization_denied"});
    let mut unavailable = managed_delivery("dispatch_unknown");
    unavailable["data"]["observation"] = serde_json::json!({"state":"observation_blocked","reason":"native_observation_unavailable"});
    let mut historical = managed_complete(true, false, "archived");
    historical["data"]["observation"] = serde_json::json!({"state":"archived","historical":true,"native_result":{"id":"ledger:1:nonce","converged":true}});
    let mut outstanding = managed_delivery("dispatched");
    outstanding["data"]["observation"] = serde_json::json!({"state":"observed","native":{"deployment":{"status":"outstanding","id":"ledger:1:nonce","input_digest":"digest","graphs":{}},"active":false,"in_progress":false}});
    let unknowns = ["not_recorded", "result_expired", "identity_mismatch", "different_ledger"].map(|status| {
        let mut body = managed_delivery("dispatch_unknown");
        body["data"]["observation"] = serde_json::json!({"state":"unknown","lookup":{"status":status},"active":false,"in_progress":false,"historical":false});
        (body, 5)
    });
    for (body, code) in [
        (managed_complete(true, true, "archived"), 0),
        (managed_complete(false, false, "archived"), 1),
        (managed_complete(true, false, "archived"), 5),
        (managed_complete(true, true, "archive_blocked"), 5),
        (managed_complete(true, true, "pending"), 5),
        (managed_delivery("dispatch_unknown"), 5),
        (managed_delivery("expired"), 1),
        (managed_delivery("cancelled"), 1),
        (unavailable, 5),
        (historical, 5),
        (blocked, 2),
        (outstanding, 5),
        (refused, 2),
    ]
    .into_iter()
    .chain(unknowns)
    {
        let temp = tempdir().unwrap();
        let api = lifecycle_fixture(vec![IntentReply::json(200, body.clone())]);
        write_managed_target(temp.path(), &api.origin);
        let output = managed_cli(temp.path(), &api.origin)
            .args([
                "status",
                "delivery-one",
                "--wait",
                "--timeout",
                "1",
                "--json",
            ])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(code), "{body}: {output:?}");
        assert_eq!(parse_stdout_json(&output), body);
        assert_control_request(
            &api.workflow_requests()[0],
            "GET",
            "/v1/clusters/managed-test/deployments/delivery-one",
            Value::Null,
            None,
        );
        api.assert_complete();
        assert_no_core_effects(temp.path());
    }
}

#[test]
fn managed_current_policy_and_stale_preview_refusals_never_submit_again() {
    for (status, kind, args) in [
        (
            403,
            "native_authorization_denied",
            vec!["plan", "--rev", MANAGED_REVISION],
        ),
        (
            409,
            "native_busy_or_stale",
            vec!["apply", "--plan", "saved-plan"],
        ),
        (
            403,
            "native_authorization_denied",
            vec!["apply", "--plan", "saved-plan"],
        ),
    ] {
        let temp = tempdir().unwrap();
        let problem = serde_json::json!({"type":kind,"status":status,"detail":"current native authority refused"});
        let api = lifecycle_fixture(vec![IntentReply::json(status, problem.clone())]);
        write_managed_target(temp.path(), &api.origin);
        let output = managed_cli(temp.path(), &api.origin)
            .args(args)
            .arg("--json")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert_eq!(parse_stdout_json(&output), problem);
        api.assert_complete();
        assert_no_core_effects(temp.path());
    }
}

#[test]
#[ignore = "nightly: four real 2 s delivery polling intervals"]
fn managed_poll_identity_mismatch_retains_original_delivery_and_native_ids() {
    for pointer in [
        "/data/deployment_id",
        "/data/native_deployment_id",
        "/data/preview_id",
        "/data/observation/native/deployment/result/id",
    ] {
        let temp = tempdir().unwrap();
        let queued = managed_delivery("queued");
        let mut changed = managed_complete(true, true, "archived");
        *changed.pointer_mut(pointer).unwrap() = serde_json::json!("foreign");
        let api = lifecycle_fixture(vec![
            IntentReply::json(202, queued),
            IntentReply::json(200, changed),
        ]);
        write_managed_target(temp.path(), &api.origin);
        let output = managed_cli(temp.path(), &api.origin)
            .args(["apply", "--plan", "saved-plan", "--json"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{output:?}");
        let problem = parse_stdout_json(&output);
        assert_eq!(problem["type"], "context_mismatch");
        assert_eq!(
            problem["accepted_deployment"]["deployment_id"],
            "delivery-one"
        );
        assert_eq!(
            problem["accepted_deployment"]["native_deployment_id"],
            "ledger:1:nonce"
        );
        assert_eq!(
            api.workflow_requests()
                .iter()
                .filter(|r| r.method == "POST")
                .count(),
            1
        );
        api.assert_complete();
        assert_no_core_effects(temp.path());
    }
}

#[test]
fn managed_lost_submission_retains_key_without_effect_retry() {
    let temp = tempdir().unwrap();
    let api = lifecycle_fixture(vec![IntentReply {
        status: 202,
        headers: vec![],
        body: b"{".to_vec(),
    }]);
    write_managed_target(temp.path(), &api.origin);
    let output = managed_cli(temp.path(), &api.origin)
        .args([
            "apply",
            "--plan",
            "saved-plan",
            "--idempotency-key",
            "original-key",
            "--json",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let result = parse_stdout_json(&output);
    assert_eq!(result["submission"]["idempotency_key"], "original-key");
    assert_eq!(result["submission"]["request"]["preview_id"], "saved-plan");
    assert_eq!(result["submission"]["acceptance"], "unknown");
    api.assert_complete();
    assert_no_core_effects(temp.path());
}

#[test]
fn managed_folder_context_is_inert_and_missing_target_never_falls_back() {
    let temp = tempdir().unwrap();
    let api = lifecycle_fixture(vec![]);
    write_cluster_config_fixture(temp.path());
    fs::create_dir(temp.path().join(".omnigraph")).unwrap();
    fs::write(temp.path().join(".omnigraph/context"), "malformed").unwrap();
    for verb in ["plan", "apply", "status"] {
        let output = output_failure(cli().current_dir(temp.path()).args(["cluster", verb]));
        assert!(String::from_utf8_lossy(&output.stderr).contains("target"));
    }
    let preview = cluster_json(temp.path(), "plan");
    assert_eq!(preview["ok"], true);
    let applied = apply_cluster_fixture(temp.path());
    assert_eq!(applied["status"], "complete");
    assert_eq!(
        fs::read(temp.path().join(".omnigraph/context")).unwrap(),
        b"malformed"
    );
    assert!(api.requests().is_empty());
}

#[test]
fn managed_cancel_uses_exact_cluster_scope_and_refuses_foreign_response() {
    let temp = tempdir().unwrap();
    let mut foreign = managed_delivery("cancelled");
    foreign["meta"]["cluster_id"] = serde_json::json!("another-cluster");
    let api = lifecycle_fixture(vec![IntentReply::json(200, foreign)]);
    write_managed_target(temp.path(), &api.origin);
    let output = managed_cli(temp.path(), &api.origin)
        .args(["cancel", "delivery-one", "--json"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(parse_stdout_json(&output)["type"], "context_mismatch");
    assert_control_request(
        &api.workflow_requests()[0],
        "POST",
        "/v1/clusters/managed-test/deployments/delivery-one:cancel",
        Value::Null,
        None,
    );
    api.assert_complete();
    assert_no_core_effects(temp.path());
}

#[test]
fn managed_cancel_is_idempotent_and_cannot_cancel_attempted_native_work() {
    let temp = tempdir().unwrap();
    let cancelled = managed_delivery("cancelled");
    let problem = serde_json::json!({"type":"native_cancellation_too_late","status":409,"detail":"an attempt may have reached the native server"});
    let api = lifecycle_fixture(vec![
        IntentReply::json(200, cancelled.clone()),
        IntentReply::json(200, cancelled.clone()),
        IntentReply::json(409, problem.clone()),
    ]);
    write_managed_target(temp.path(), &api.origin);
    for (expected, code) in [(cancelled.clone(), 0), (cancelled, 0), (problem, 2)] {
        let output = managed_cli(temp.path(), &api.origin)
            .args(["cancel", "delivery-one", "--json"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(code));
        let mut expected = expected;
        if code == 2 {
            expected["requested_deployment_id"] = serde_json::json!("delivery-one");
        }
        assert_eq!(parse_stdout_json(&output), expected);
    }
    for request in api.workflow_requests() {
        assert_control_request(
            &request,
            "POST",
            "/v1/clusters/managed-test/deployments/delivery-one:cancel",
            Value::Null,
            None,
        );
    }
    api.assert_complete();
    assert_no_core_effects(temp.path());
}

#[test]
fn managed_redirect_and_oversized_reply_fail_without_following_or_core_effects() {
    let redirect_target = lifecycle_fixture(vec![]);
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
        let api = lifecycle_fixture(vec![IntentReply {
            status,
            headers,
            body: vec![],
        }]);
        write_managed_target(temp.path(), &api.origin);
        let output = managed_cli(temp.path(), &api.origin)
            .args(["apply", "--plan", "saved-plan", "--json"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1));
        assert_eq!(parse_stdout_json(&output)["type"], expected);
        assert_eq!(api.workflow_requests().len(), 1);
        assert_no_core_effects(temp.path());
    }
    assert!(redirect_target.requests().is_empty());
}

#[test]
fn managed_auth_selector_and_api_down_never_open_core() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    let api = lifecycle_fixture(vec![]);
    write_managed_target(temp.path(), &api.origin);
    let mismatch = managed_cli(temp.path(), &api.origin)
        .args(["--auth", "unknown-profile"])
        .args(["apply", "--plan", "saved-plan", "--json"])
        .output()
        .unwrap();
    assert_eq!(mismatch.status.code(), Some(64), "{mismatch:?}");
    assert!(mismatch.stdout.is_empty(), "{mismatch:?}");
    assert!(
        String::from_utf8_lossy(&mismatch.stderr).contains("unexpected argument '--auth'"),
        "{mismatch:?}"
    );
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
    apply_cluster_fixture(temp.path());
    fs::write(temp.path().join("people.gq"),
        "query find_person($name: String) { match { $p: Person { name: $name } } return { $p.name } }").unwrap();
    let json = cluster_json(temp.path(), "plan");
    assert_eq!(json["ok"], true);
    assert_eq!(json["state_observations"]["state_found"], true);
    assert!(
        json["changes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|change| change["resource"] == "query.knowledge.find_person"
                && change["operation"] == "update"),
        "{json}"
    );
}

#[test]
fn cluster_status_json_reports_missing_state() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    let output = output_failure(
        cli()
            .args(["cluster", "status", "--target"])
            .arg(temp.path())
            .arg("--json"),
    );
    assert_eq!(
        parse_stdout_json(&output)["diagnostics"][0]["code"],
        "state_missing"
    );
    assert!(!temp.path().join("__cluster/state.json").exists());
}

#[test]
fn cluster_status_json_reports_applied_revision_and_exact_lock() {
    let temp = applied_knowledge_cluster();
    write_cluster_lock(temp.path(), "held-lock", "refresh");
    let state: Value =
        serde_json::from_slice(&fs::read(temp.path().join("__cluster/state.json")).unwrap())
            .unwrap();
    fs::remove_file(temp.path().join("cluster.yaml")).unwrap();
    let output = output_success(
        cli()
            .args(["cluster", "status", "--target"])
            .arg(temp.path())
            .arg("--json"),
    );
    let json = parse_stdout_json(&output);
    assert_eq!(json["lock_id"], "held-lock");
    assert_eq!(json["state_revision"], state["state_revision"]);
    assert_eq!(
        json["result_revision"],
        state["applied_revision"]["result_revision"]
    );
    assert_eq!(json["next_sequence"], state["next_sequence"]);
}

#[test]
fn cluster_plan_json_includes_state_cas_revision_and_lock_observation() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    apply_cluster_fixture(temp.path());
    let state_path = temp.path().join("__cluster/state.json");
    let before = fs::read(&state_path).unwrap();
    let state: Value = serde_json::from_slice(&before).unwrap();
    let json = cluster_json(temp.path(), "plan");
    assert_eq!(json["ok"], true);
    assert_eq!(
        json["state_observations"]["state_revision"],
        state["state_revision"]
    );
    assert!(
        json["state_observations"]["state_cas"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    assert_eq!(json["state_observations"]["locked"], false);
    assert!(!temp.path().join("__cluster/lock.json").exists());
    assert_eq!(fs::read(&state_path).unwrap(), before);
}

#[test]
fn cluster_plan_observes_an_existing_lock() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    write_cluster_lock(temp.path(), "held-lock", "plan");

    let output = output_success(
        cli()
            .arg("cluster")
            .arg("plan")
            .arg("--target")
            .arg(temp.path())
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    );
    let json = parse_stdout_json(&output);
    assert_eq!(json["ok"], true);
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
    assert_eq!(json["authority"], "observed");
    assert!(temp.path().join("__cluster/lock.json").exists());
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
            .arg("--target")
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(json["unlocked"], true);
    assert_eq!(json["lock_id"], "held-lock");
    assert!(!temp.path().join("__cluster/lock.json").exists());
}

#[test]
fn cluster_force_unlock_wrong_id_exits_nonzero() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    write_cluster_lock(temp.path(), "held-lock", "plan");
    let lock_path = temp.path().join("__cluster/lock.json");
    let before = fs::read(&lock_path).unwrap();

    let json = parse_stdout_json(&output_failure(
        cli()
            .arg("cluster")
            .arg("force-unlock")
            .arg("other-lock")
            .arg("--target")
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(json["ok"], false);
    assert!(
        json["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|diagnostic| diagnostic["code"] == "state_lock_id_mismatch")
    );
    assert_eq!(fs::read(&lock_path).unwrap(), before);
}

#[test]
fn cluster_plan_succeeds_before_and_after_force_unlock() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    write_cluster_lock(temp.path(), "held-lock", "plan");

    let locked = parse_stdout_json(&output_success(
        cli()
            .arg("cluster")
            .arg("plan")
            .arg("--target")
            .arg(temp.path())
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(locked["ok"], true);
    assert_eq!(locked["state_observations"]["lock_id"], "held-lock");

    let unlocked = parse_stdout_json(&output_success(
        cli()
            .arg("cluster")
            .arg("force-unlock")
            .arg("held-lock")
            .arg("--target")
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(unlocked["unlocked"], true);

    let planned = parse_stdout_json(&output_success(
        cli()
            .arg("cluster")
            .arg("plan")
            .arg("--target")
            .arg(temp.path())
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(planned["ok"], true);
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
    let validate = cluster_json(temp.path(), "validate");
    let json = apply_cluster_fixture(temp.path());
    assert_eq!(json["result"]["graphs"]["knowledge"]["outcome"], "created");
    let state: Value =
        serde_json::from_slice(&fs::read(temp.path().join("__cluster/state.json")).unwrap())
            .unwrap();
    assert_eq!(state["version"], 2);
    for resource in ["query.knowledge.find_person", "policy.base"] {
        assert_eq!(
            state["applied_revision"]["resources"][resource]["digest"],
            validate["resource_digests"][resource]
        );
    }
    let query_digest = validate["resource_digests"]["query.knowledge.find_person"]
        .as_str()
        .unwrap();
    assert!(
        temp.path()
            .join("__cluster/resources/query/knowledge/find_person")
            .join(format!("{query_digest}.gq"))
            .exists()
    );
}

#[test]
fn cluster_apply_bootstraps_v2_and_retains_exact_owner() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    let output = output_success(
        cli()
            .arg("--target")
            .arg(temp.path())
            .args(["cluster", "apply", "--config"])
            .arg(temp.path())
            .arg("--json"),
    );
    let receipt = parse_stdout_json(&output);
    assert_eq!(receipt["status"], "complete");
    assert_eq!(receipt["result"]["converged"], true);
    assert!(
        String::from_utf8_lossy(&output.stderr).contains(receipt["result"]["id"].as_str().unwrap())
    );
    assert!(
        temp.path()
            .join("graphs/knowledge.omni/__manifest")
            .exists()
    );
    assert!(temp.path().join("__cluster/lock.json").exists());
}

#[test]
fn cluster_apply_locked_exits_nonzero() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    apply_cluster_fixture(temp.path());
    let before = fs::read(temp.path().join("__cluster/state.json")).unwrap();
    write_cluster_lock(temp.path(), "held-lock", "plan");
    let output = output_failure(
        cli()
            .arg("--target")
            .arg(temp.path())
            .args(["cluster", "apply", "--config"])
            .arg(temp.path())
            .arg("--json"),
    );
    assert_eq!(
        parse_stdout_json(&output)["diagnostics"][0]["code"],
        "state_lock_held"
    );
    assert_eq!(
        fs::read(temp.path().join("__cluster/state.json")).unwrap(),
        before
    );
    assert!(temp.path().join("__cluster/lock.json").exists());
}

#[test]
fn cluster_apply_uses_explicit_actor_with_version1_operator_home() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    let operator_home = tempdir().unwrap();
    fs::write(operator_home.path().join("config.yaml"), "version: 1\n").unwrap();
    for (extra, expected) in [
        (vec!["--as", "act-operator"], "act-operator"),
        (vec!["--as", "andrew"], "andrew"),
    ] {
        let output = output_success(
            cli()
                .current_dir(temp.path())
                .env("OMNIGRAPH_HOME", operator_home.path())
                .args(extra)
                .arg("--target")
                .arg(temp.path())
                .args(["cluster", "apply", "--config"])
                .arg(temp.path())
                .arg("--json"),
        );
        let receipt = parse_stdout_json(&output);
        assert_eq!(
            receipt["result"]["authority"]["actor"], expected,
            "{receipt}"
        );
        unlock_cluster_fixture(temp.path());
    }
}

#[test]
fn cluster_commands_ignore_legacy_omnigraph_yaml() {
    // RFC-011: the CLI never reads omnigraph.yaml for cluster commands — a
    // present (even malformed) legacy file is inert. No actor is inferred.
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    fs::write(temp.path().join("omnigraph.yaml"), "{{{{ not yaml").unwrap();

    for command in ["validate", "plan"] {
        let mut invocation = cli();
        invocation
            .current_dir(temp.path())
            .args(["cluster", command, "--config"])
            .arg(temp.path())
            .arg("--json");
        if command == "plan" {
            invocation.arg("--target").arg(temp.path());
        }
        let output = invocation.output().unwrap();
        assert!(output.status.success(), "{output:?}");
        assert!(!String::from_utf8_lossy(&output.stderr).contains("omnigraph.yaml"));
    }
    // Bootstrap apply (no --as, no operator config): the legacy file is never
    // loaded and the no-actor apply succeeds (actor defaults to none).
    let output = cli()
        .current_dir(temp.path())
        .arg("--target")
        .arg(temp.path())
        .args(["cluster", "apply", "--config"])
        .arg(temp.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "cluster apply affected by malformed omnigraph.yaml: {}",
        String::from_utf8_lossy(&output.stderr)
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
/// its directory guard. Uses the production v2 bootstrap and releases its completed fixture owner.
fn applied_knowledge_cluster() -> tempfile::TempDir {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    apply_cluster_fixture(temp.path());
    temp
}

#[test]
fn optimize_resolves_a_cluster_graph_by_id() {
    let temp = applied_knowledge_cluster();
    // No hand-typed storage path: address the graph by cluster dir + id.
    let out = output_success(
        cli()
            .arg("optimize")
            .arg("--target")
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

fn maintenance_status() -> Value {
    serde_json::json!({"status":{"ledger_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","next_sequence":1,"outstanding_id":null},
        "limits":{"max_tables":32,"max_fragments":1000,"max_rows":100000,"max_source_bytes":10000000,"max_files":2000}})
}

fn maintenance_receipt(request: &Value, complete: bool) -> Value {
    let invocation = serde_json::json!({"request":request,"actor":"act-writer","canonical_root":"file:///server-only-storage",
        "result_revision":1,"input":null,"publication":null,"settlement":null,"schema_contract":{"source_hash":"schema","schema_ir_hash":"ir","schema_identity_domain":"domain","schema_identity_version":1}});
    if complete {
        serde_json::json!({"maintenance":{"status":"complete","result":{
            "operation_id":request["operation_id"],"graph":request["graph"],"invocation":invocation,
            "outcome":"success","report":{"before_commit":"before","after_commit":"after","tables":[],
                "source_tables":0,"source_fragments":0,"source_rows":0,"source_bytes":0,"source_files":0}}},"in_progress":false})
    } else {
        serde_json::json!({"maintenance":{"status":"outstanding","operation_id":request["operation_id"],
            "graph":request["graph"],"invocation":invocation},"in_progress":true})
    }
}

#[test]
fn served_optimize_submits_once_and_observes_the_original_identity() {
    for mode in [
        "wait",
        "no_wait",
        "lost_response",
        "mismatched_request",
        "no_wait_before_acceptance",
    ] {
        let mut submitted = None;
        let mut lookups = 0;
        let api = IntentApiFixture::graph_responding(
            match mode {
                "no_wait" => 2,
                "no_wait_before_acceptance" => 4,
                _ => 3,
            },
            move |request| match (request.method.as_str(), request.path.as_str()) {
                ("GET", "/cluster/maintenance?graph=knowledge") => {
                    IntentReply::json(200, maintenance_status())
                }
                ("POST", "/cluster/maintenance") => {
                    assert!(submitted.is_none(), "submission must never replay");
                    submitted = Some(request.body.clone());
                    if matches!(
                        mode,
                        "lost_response" | "mismatched_request" | "no_wait_before_acceptance"
                    ) {
                        IntentReply::json(504, serde_json::json!({"error":"lost response"}))
                    } else {
                        IntentReply::json(202, maintenance_receipt(&request.body, false))
                    }
                }
                ("GET", path) => {
                    lookups += 1;
                    let body = submitted.as_ref().unwrap();
                    assert_eq!(
                        path,
                        format!(
                            "/cluster/maintenance/{}?graph=knowledge",
                            body["operation_id"].as_str().unwrap()
                        )
                    );
                    if mode == "no_wait_before_acceptance" && lookups == 1 {
                        return IntentReply::json(
                            200,
                            serde_json::json!({"maintenance":{"status":"not_recorded","operation_id":body["operation_id"]},"in_progress":true}),
                        );
                    }
                    let mut receipt =
                        maintenance_receipt(body, mode != "no_wait_before_acceptance");
                    if mode == "mismatched_request" {
                        receipt["maintenance"]["result"]["invocation"]["request"]["limits"]["max_rows"] =
                            1.into();
                    }
                    IntentReply::json(200, receipt)
                }
                _ => panic!("unexpected request: {request:?}"),
            },
        );
        let operator = KeyedServer::new(&api.origin);
        let mut command = cli();
        command.envs(operator.env("optimization-token")).args([
            "optimize",
            "--target",
            KEYED_SERVER_NAME,
            "--graph",
            "knowledge",
            "--json",
            "--timeout",
            "5",
        ]);
        if mode.starts_with("no_wait") {
            command.arg("--no-wait");
        }
        let output = command.output().unwrap();
        assert_eq!(
            output.status.success(),
            mode != "mismatched_request",
            "{mode}: {output:?}"
        );
        let requests = api.workflow_requests();
        let posts: Vec<_> = requests
            .iter()
            .filter(|request| request.method == "POST")
            .collect();
        assert_eq!(posts.len(), 1);
        let id = posts[0].body["operation_id"].as_str().unwrap();
        assert!(id.starts_with("01ARZ3NDEKTSV4RRFFQ69G5FAV:1:"));
        assert_eq!(id.len(), 55);
        assert!(String::from_utf8_lossy(&output.stderr).contains(id));
        assert_eq!(posts[0].body["limits"], maintenance_status()["limits"]);
        assert_eq!(posts[0].body["kind"], "optimize");
        for request in &requests {
            assert_eq!(
                request.headers.get("authorization").map(String::as_str),
                Some("Bearer optimization-token")
            );
        }
        if mode != "mismatched_request" {
            assert_eq!(
                parse_stdout_json(&output)["maintenance"]["status"],
                if mode.starts_with("no_wait") {
                    "outstanding"
                } else {
                    "complete"
                }
            );
        }
        assert_no_core_effects(operator.home());
        api.assert_complete();
    }
}

#[test]
fn served_optimize_operation_id_is_get_only_even_when_not_recorded_or_expired() {
    let id = "01ARZ3NDEKTSV4RRFFQ69G5FAV:1:01ARZ3NDEKTSV4RRFFQ69G5FAW";
    for status in [
        "not_recorded",
        "result_expired",
        "identity_mismatch",
        "different_ledger",
    ] {
        let response = serde_json::json!({"maintenance":{"status":status,"operation_id":id},"in_progress":false});
        let api = IntentApiFixture::graph(vec![IntentReply::json(200, response.clone())]);
        let output = output_failure(
            cli()
                .args(["optimize", "--target"])
                .fixture_target(&api.origin)
                .args(["--graph", "knowledge", "--operation-id", id, "--json"]),
        );
        assert_eq!(parse_stdout_json(&output), response);
        let requests = api.workflow_requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "GET");
        assert_eq!(
            requests[0].path,
            format!("/cluster/maintenance/{id}?graph=knowledge")
        );
        api.assert_complete();
    }
}

#[test]
fn served_optimize_timeout_never_cancels_or_resubmits() {
    let id = "01ARZ3NDEKTSV4RRFFQ69G5FAV:1:01ARZ3NDEKTSV4RRFFQ69G5FAW";
    let api = IntentApiFixture::graph_with_response_delay(
        vec![IntentReply::json(
            200,
            serde_json::json!({"maintenance":{"status":"not_recorded","operation_id":id},"in_progress":false}),
        )],
        std::time::Duration::from_secs(2),
    );
    let output = output_failure(
        cli()
            .args(["optimize", "--target"])
            .fixture_target(&api.origin)
            .args([
                "--graph",
                "knowledge",
                "--operation-id",
                id,
                "--timeout",
                "1",
                "--json",
            ]),
    );
    assert_eq!(output.status.code(), Some(5), "{output:?}");
    assert_eq!(parse_stdout_json(&output)["outcome"], "wait_timeout");
    let requests = api.workflow_requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "GET");
    api.assert_complete();
}

#[test]
fn served_optimize_preserves_refusal_and_unresolved_receipts_without_success() {
    let id = "01ARZ3NDEKTSV4RRFFQ69G5FAV:1:01ARZ3NDEKTSV4RRFFQ69G5FAW";
    let request = serde_json::json!({"operation_id":id,"graph":"knowledge","kind":"optimize","limits":maintenance_status()["limits"]});
    for outcome in ["refused", "outstanding", "unpublished", "published"] {
        let mut response = maintenance_receipt(&request, outcome != "outstanding");
        response["in_progress"] = false.into();
        if outcome == "refused" {
            let result = response["maintenance"]["result"].as_object_mut().unwrap();
            result.remove("report");
            result.insert("outcome".into(), "refused".into());
            result.insert("code".into(), "optimization_preflight_refused".into());
            result.insert("message".into(), "source exceeds configured ceiling".into());
        } else if matches!(outcome, "unpublished" | "published") {
            let result = response["maintenance"]["result"].as_object_mut().unwrap();
            let report = result.remove("report").unwrap();
            result.insert("outcome".into(), "interrupted".into());
            result.insert(
                "publication".into(),
                if outcome == "published" {
                    serde_json::json!({"status":"published","report":report})
                } else {
                    serde_json::json!({"status":"unpublished"})
                },
            );
        }
        let api = IntentApiFixture::graph(vec![IntentReply::json(200, response.clone())]);
        let output = output_failure(
            cli()
                .args(["optimize", "--target"])
                .fixture_target(&api.origin)
                .args([
                    "--graph",
                    "knowledge",
                    "--operation-id",
                    id,
                    "--no-wait",
                    "--json",
                ]),
        );
        assert_eq!(parse_stdout_json(&output), response);
        assert_eq!(api.workflow_requests().len(), 1);
        assert_eq!(api.workflow_requests()[0].method, "GET");
        api.assert_complete();
    }
}

#[test]
fn optimize_refuses_inapplicable_flags_and_managed_targets_before_requests() {
    let api = IntentApiFixture::graph(vec![]);
    for extra in [
        vec!["--as", "act-other"],
        vec!["--branch", "review"],
        vec!["--timeout", "0"],
        vec!["--timeout", "3601"],
        vec!["--operation-id", "original", "--writers-stopped"],
    ] {
        output_failure(
            cli()
                .args(["optimize", "--target"])
                .fixture_target(&api.origin)
                .args(["--graph", "knowledge"])
                .args(extra),
        );
    }
    let root = tempdir().unwrap();
    for args in [
        vec!["--no-wait"],
        vec!["--operation-id", "original"],
        vec!["--timeout", "1"],
        vec!["--writers-stopped"],
        vec![
            "--operation-id",
            "original",
            "--writers-stopped",
            "--no-wait",
        ],
        vec![
            "--operation-id",
            "original",
            "--writers-stopped",
            "--timeout",
            "1",
        ],
    ] {
        output_failure(
            cli()
                .args(["optimize", "--target"])
                .arg(root.path().join("must-not-create"))
                .args(args),
        );
        assert!(!root.path().join("must-not-create").exists());
    }
    let managed = IntentApiFixture::new(vec![]);
    let output = output_failure(
        managed_fixture_command(root.path(), &managed.origin, "managed-test")
            .arg("optimize")
            .args(["--graph", "knowledge", "--json"]),
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("target_kind_unsupported"),
        "{output:?}"
    );
    assert!(managed.workflow_requests().is_empty());
    api.assert_complete();
    managed.assert_complete();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn optimize_recovery_requires_exclusion_and_reconciles_the_original_receipt() {
    use omnigraph_cluster::{
        ClusterAdmissionPurpose, IdentityAuthorization, MaintenanceAcceptance, MaintenanceKind,
        MaintenanceRequest,
    };
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    let policy = temp.path().join("base.policy.yaml");
    fs::write(
        &policy,
        fs::read_to_string(&policy).unwrap().replace(
            "actions: [change, schema_apply]",
            "actions: [change, schema_apply, optimize]",
        ),
    )
    .unwrap();
    apply_cluster_fixture(temp.path());
    let root = temp.path().to_str().unwrap();
    let actor = IdentityAuthorization::authenticated("act-cluster-test").unwrap();
    let owner = omnigraph_cluster::acquire_cluster_admission(root, ClusterAdmissionPurpose::Serve)
        .await
        .unwrap()
        .unwrap();
    let id = omnigraph_cluster::maintenance_status(&owner, "knowledge", &actor)
        .await
        .unwrap()
        .next_operation_id();
    let request = MaintenanceRequest {
        operation_id: id.clone(),
        graph: "knowledge".into(),
        kind: MaintenanceKind::Optimize,
        limits: serde_json::from_value(maintenance_status()["limits"].clone()).unwrap(),
    };
    let MaintenanceAcceptance::Accepted(token) =
        omnigraph_cluster::accept_maintenance(&owner, request, &actor)
            .await
            .unwrap()
    else {
        panic!("fresh acceptance")
    };
    let command = || {
        let mut command = cli();
        command.args([
            "optimize",
            "--target",
            root,
            "--graph",
            "knowledge",
            "--as",
            "act-cluster-test",
            "--operation-id",
            &id,
            "--writers-stopped",
            "--json",
        ]);
        command
    };
    let refused = output_failure(&mut command());
    assert!(
        String::from_utf8_lossy(&refused.stdout).contains("state_lock_held")
            || String::from_utf8_lossy(&refused.stderr).contains("state_lock_held"),
        "{refused:?}"
    );
    // This fixture accepted no physical work. Drop every owner before the
    // explicit administrator unlock; the recovery flag never unlocks it.
    let lock = owner.lock_id().to_string();
    drop(token);
    drop(owner);
    omnigraph_cluster::force_unlock_storage_root(root, &lock)
        .await
        .unwrap();
    let journal = temp.path().join("__cluster/maintenance.json");
    let before = fs::read(&journal).unwrap();
    let recovered = parse_stdout_json(&output_success(&mut command()));
    assert_eq!(recovered["maintenance"]["result"]["operation_id"], id);
    assert_eq!(recovered["maintenance"]["result"]["outcome"], "interrupted");
    assert_eq!(
        recovered["maintenance"]["result"]["publication"]["status"],
        "unpublished"
    );
    assert_ne!(fs::read(&journal).unwrap(), before);
    assert_eq!(
        parse_stdout_json(&output_success(&mut command())),
        recovered
    );
    let successor =
        omnigraph_cluster::acquire_cluster_admission(root, ClusterAdmissionPurpose::Serve)
            .await
            .unwrap()
            .unwrap();
    successor.release_after_settlement().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v2_root_admission_blocks_cli_write_and_native_control_doors() {
    let temp = applied_knowledge_cluster();
    let root = format!("file://{}", temp.path().display());
    let owner = omnigraph_cluster::acquire_cluster_admission(
        &root,
        omnigraph_cluster::ClusterAdmissionPurpose::Serve,
    )
    .await
    .unwrap()
    .unwrap();
    let graph = temp.path().join("graphs/knowledge.omni");
    let before = fs::read(temp.path().join("__cluster/state.json")).unwrap();
    for args in [
        vec!["optimize", "--json"],
        vec!["cleanup", "--keep", "1", "--confirm", "--json"],
        vec!["branch", "create", "must-not-exist"],
    ] {
        let output = output_failure(cli().args(&args).arg("--target").arg(&graph).arg("--yes"));
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("state_lock_held"),
            "{args:?}: {output:?}"
        );
    }
    assert_eq!(
        fs::read(temp.path().join("__cluster/state.json")).unwrap(),
        before
    );
    owner.release_after_settlement().await.unwrap();

    // A raw writer can leave text-equal but identity-different schema drift.
    // Read-only opening must reject that state without acquiring a lock.
    let db = omnigraph::db::Omnigraph::open(graph.to_str().unwrap())
        .await
        .unwrap();
    let source = db.schema_source();
    let original_contract = db.schema_contract_digest();
    db.apply_schema("node Person { name: String @key }")
        .await
        .unwrap();
    db.apply_schema(&source).await.unwrap();
    assert_eq!(db.schema_source(), source);
    assert_ne!(db.schema_contract_digest(), original_contract);
    let refused = output_failure(cli().args(["schema", "show", "--target"]).arg(&graph));
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("applied_schema_drift"),
        "{refused:?}"
    );
    assert!(!temp.path().join("__cluster/lock.json").exists());
    assert_eq!(
        fs::read(temp.path().join("__cluster/state.json")).unwrap(),
        before
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn successful_v2_cli_reads_preserve_admission_and_applied_state() {
    let temp = applied_knowledge_cluster();
    let root = format!("file://{}", temp.path().display());
    let graph = temp.path().join("graphs/knowledge.omni");
    let state_path = temp.path().join("__cluster/state.json");
    let lock_path = temp.path().join("__cluster/lock.json");
    let before = fs::read(&state_path).unwrap();
    let schema = temp.path().join("people.pg");
    let query = temp.path().join("people.gq");
    for held in [false, true] {
        let owner = if held {
            omnigraph_cluster::acquire_cluster_admission(
                &root,
                omnigraph_cluster::ClusterAdmissionPurpose::Serve,
            )
            .await
            .unwrap()
        } else {
            None
        };
        let before_lock = fs::read(&lock_path).ok();
        let mut reads = vec![
            vec!["schema", "show"],
            vec!["schema", "plan", "--schema", schema.to_str().unwrap()],
            vec!["lint", "--query", query.to_str().unwrap()],
            vec!["branch", "list"],
            vec!["commit", "list"],
            vec!["export"],
            vec![
                "query",
                "find_person",
                "--query",
                query.to_str().unwrap(),
                "--params",
                r#"{"name":"Alice"}"#,
            ],
        ];
        for args in &mut reads {
            args.extend(["--target", graph.to_str().unwrap()]);
        }
        reads.push(vec![
            "queries",
            "validate",
            "--target",
            temp.path().to_str().unwrap(),
        ]);
        for args in reads {
            let output = output_success(cli().args(&args));
            assert!(
                !String::from_utf8_lossy(&output.stderr).contains("admission retained"),
                "{args:?}: {output:?}"
            );
            assert_eq!(fs::read(&lock_path).ok(), before_lock, "{args:?}");
            assert_eq!(fs::read(&state_path).unwrap(), before, "{args:?}");
        }
        if let Some(owner) = owner {
            owner.release_after_settlement().await.unwrap();
        }
    }
    // Stored-query validation must not combine an older registry with a newer
    // ledger, and an observation cannot validate another graph's handle.
    let snapshot = omnigraph_cluster::read_serving_snapshot_from_storage(&root)
        .await
        .unwrap();
    let authority = omnigraph_cluster::GraphReadAuthority::capture(
        graph.to_str().unwrap(),
        snapshot.state_cas.as_deref(),
    )
    .await
    .unwrap()
    .unwrap();
    let db = omnigraph::db::Omnigraph::open_read_only(graph.to_str().unwrap())
        .await
        .unwrap();
    authority.validate_opened(&db).await.unwrap();
    let mut changed: serde_json::Value = serde_json::from_slice(&before).unwrap();
    changed["state_revision"] = serde_json::json!(changed["state_revision"].as_u64().unwrap() + 1);
    fs::write(&state_path, serde_json::to_vec(&changed).unwrap()).unwrap();
    assert_eq!(
        authority.validate_opened(&db).await.unwrap_err().code,
        "cluster_read_revision_changed"
    );
    assert_eq!(
        omnigraph_cluster::GraphReadAuthority::capture(
            graph.to_str().unwrap(),
            snapshot.state_cas.as_deref(),
        )
        .await
        .unwrap_err()
        .code,
        "cluster_read_revision_changed"
    );
    fs::write(&state_path, &before).unwrap();
    let unrelated = tempdir().unwrap();
    let other = omnigraph::db::Omnigraph::init(
        unrelated.path().to_str().unwrap(),
        &fs::read_to_string(&schema).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(other.schema_source(), db.schema_source());
    assert_eq!(
        authority.validate_opened(&other).await.unwrap_err().code,
        "cluster_graph_root_mismatch"
    );
    assert!(!lock_path.exists());
    // Reads leave the writer door usable; writers still retain their own lock.
    output_success(
        cli()
            .args(["branch", "create", "after-reads", "--target"])
            .arg(&graph),
    );
    assert!(lock_path.exists());
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v2_graph_alias_cannot_bypass_server_admission() {
    let temp = applied_knowledge_cluster();
    let root = format!("file://{}", temp.path().display());
    let owner = omnigraph_cluster::acquire_cluster_admission(
        &root,
        omnigraph_cluster::ClusterAdmissionPurpose::Serve,
    )
    .await
    .unwrap()
    .unwrap();
    let alias_dir = tempdir().unwrap();
    let alias = alias_dir.path().join("arbitrary-name");
    std::os::unix::fs::symlink(temp.path().join("graphs/knowledge.omni"), &alias).unwrap();
    let before_state = fs::read(temp.path().join("__cluster/state.json")).unwrap();
    let before_lock = fs::read(temp.path().join("__cluster/lock.json")).unwrap();
    output_success(cli().args(["schema", "show", "--target"]).arg(&alias));
    let output = output_failure(
        cli()
            .args(["branch", "create", "blocked", "--target"])
            .arg(&alias),
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("state_lock_held"),
        "{output:?}"
    );
    let unknown = temp.path().join("graphs/unknown.omni");
    fs::create_dir(&unknown).unwrap();
    let escaped = temp.path().join("graphs/escaped.omni");
    let outside = alias_dir.path().join("outside.omni");
    init_graph(&outside);
    std::os::unix::fs::symlink(&outside, &escaped).unwrap();
    for (graph, code) in [
        (&unknown, "graph_not_applied"),
        (&escaped, "cluster_graph_root_mismatch"),
    ] {
        let output = output_failure(cli().args(["schema", "show", "--target"]).arg(graph));
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(code),
            "{output:?}"
        );
    }
    assert_eq!(
        fs::read(temp.path().join("__cluster/state.json")).unwrap(),
        before_state
    );
    assert_eq!(
        fs::read(temp.path().join("__cluster/lock.json")).unwrap(),
        before_lock
    );
    owner.release_after_settlement().await.unwrap();
}

#[test]
fn optimize_unknown_cluster_graph_id_errors() {
    let temp = applied_knowledge_cluster();
    let out = output_failure(
        cli()
            .arg("optimize")
            .arg("--target")
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
fn optimize_uses_only_an_explicit_or_configured_graph() {
    let temp = applied_knowledge_cluster();
    let refused = output_failure(
        cli()
            .args(["optimize", "--target"])
            .arg(temp.path())
            .arg("--json"),
    );
    assert!(String::from_utf8_lossy(&refused.stderr).contains("graph"));
    let home = tempdir().unwrap();
    fs::write(home.path().join("config.yaml"), format!("version: 1\ntargets:\n  root:\n    kind: direct\n    scope: root\n    uri: file://{}\n    default_graph: knowledge\n", temp.path().display())).unwrap();
    let out = output_success(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .args(["optimize", "--target", "root", "--json"]),
    );
    assert!(parse_stdout_json(&out)["datasets"].is_array());
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
    apply_cluster_fixture(root);
    temp
}

#[test]
fn optimize_on_multi_graph_cluster_requires_a_graph_without_discovery() {
    let temp = applied_two_graph_cluster();
    let operator_home = tempdir().unwrap();
    fs::write(
        operator_home.path().join("config.yaml"),
        serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "targets": {"root": {"kind": "direct", "scope": "root", "uri": url::Url::from_directory_path(temp.path()).unwrap().as_str()}}
        }))
        .unwrap(),
    )
    .unwrap();
    let out = output_failure(
        cli()
            .env("OMNIGRAPH_HOME", operator_home.path())
            .args(["optimize", "--target", "root", "--json"]),
    );
    assert_eq!(out.status.code(), Some(2));
    let refusal = parse_stdout_json(&out);
    assert_eq!(refusal["code"], "target_incompatible");
    let message = refusal["error"].as_str().unwrap();
    assert!(
        message.contains("--graph") && message.contains("default_graph"),
        "{refusal}"
    );
}

#[test]
fn init_refuses_a_cluster_managed_path_and_signposts_cluster_apply() {
    let temp = applied_knowledge_cluster();
    // Hand-init a NEW graph into the established cluster's storage layout.
    for schema in ["people.pg", "missing.pg"] {
        let out = output_failure(
            cli()
                .arg("init")
                .arg("--schema")
                .arg(temp.path().join(schema))
                .arg(temp.path().join("graphs").join("sneaky.omni")),
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("cluster apply"),
            "cluster ownership must be checked before reading {schema}; got: {stderr}"
        );
        assert!(out.stdout.is_empty());
        assert!(!temp.path().join("graphs").join("sneaky.omni").exists());
    }
}

#[test]
fn schema_apply_refuses_a_cluster_managed_graph_and_signposts_cluster_apply() {
    // RFC-011 Decision 10: a direct `schema apply` against a cluster-managed
    // graph's storage root would bypass the deployment ledger, so it is
    // refused and points at `cluster apply` (mirrors `init`'s refusal).
    let temp = applied_knowledge_cluster();
    // A schema that WOULD change the graph (adds `bio`) — so the no-mutation
    // assertion below is meaningful, not a no-op re-apply.
    fs::write(
        temp.path().join("people_v2.pg"),
        "node Person {\n  name: String @key\n  age: I32?\n  bio: String?\n}\n",
    )
    .unwrap();
    let graph = temp.path().join("graphs").join("knowledge.omni");
    let before = parse_stdout_json(&output_success(
        cli()
            .arg("snapshot")
            .arg("--target")
            .arg(&graph)
            .arg("--json"),
    ));
    for schema in ["people_v2.pg", "missing.pg"] {
        let out = output_failure(
            cli()
                .arg("schema")
                .arg("apply")
                .arg("--schema")
                .arg(temp.path().join(schema))
                .arg("--target")
                .arg(&graph),
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("cluster apply"),
            "cluster ownership must be checked before reading {schema}; got: {stderr}"
        );
        assert!(out.stdout.is_empty());
        let show = output_success(cli().args(["schema", "show", "--target"]).arg(&graph));
        assert!(
            !stdout_string(&show).contains("bio"),
            "the refused apply must not have changed the live schema; got: {}",
            stdout_string(&show)
        );
        let after = parse_stdout_json(&output_success(
            cli()
                .arg("snapshot")
                .arg("--target")
                .arg(&graph)
                .arg("--json"),
        ));
        assert_eq!(after, before, "refusal must not publish a graph change");
    }
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
    // read. This is what keeps `repair --target ROOT --graph GRAPH` usable on a degraded cluster.
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
            .arg("--target")
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

#[test]
fn managed_invalid_acknowledgements_keep_request_identity_in_json_and_human_output() {
    for (command, mut body, field) in [
        ("apply", managed_delivery("queued"), "/data/preview_id"),
        ("apply", managed_delivery("queued"), "/meta/cluster_id"),
        ("apply", managed_delivery("queued"), "/meta/incarnation"),
        ("plan", managed_preview("ready"), "/data/revision"),
        ("plan", managed_preview("ready"), "/meta/incarnation"),
    ] {
        *body.pointer_mut(field).unwrap() = serde_json::json!("foreign");
        for json in [false, true] {
            let temp = tempdir().unwrap();
            let api = lifecycle_fixture(vec![IntentReply::json(200, body.clone())]);
            write_managed_target(temp.path(), &api.origin);
            let mut cli = managed_cli(temp.path(), &api.origin);
            cli.args(if command == "apply" {
                vec!["apply", "--plan", "saved-plan"]
            } else {
                vec!["plan", "--rev", MANAGED_REVISION]
            })
            .args(["--idempotency-key", "original-key"]);
            if json {
                cli.arg("--json");
            }
            let output = cli.output().unwrap();
            assert_eq!(output.status.code(), Some(2));
            if json {
                let body = parse_stdout_json(&output);
                assert_eq!(body["submission"]["idempotency_key"], "original-key");
                assert_eq!(body["submission"]["acceptance"], "unknown");
                assert!(body.get("accepted_deployment").is_none());
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr);
                assert!(stderr.contains("submission:") && stderr.contains("original-key"));
            }
            api.assert_complete();
            assert_no_core_effects(temp.path());
        }
    }
}

#[test]
fn managed_cancellation_never_claims_an_attempted_delivery_was_cancelled() {
    let temp = tempdir().unwrap();
    let mut body = managed_delivery("cancelled");
    body["data"]["attempted_at"] = serde_json::json!("2026-10-08T00:00:00Z");
    let api = lifecycle_fixture(vec![IntentReply::json(200, body)]);
    write_managed_target(temp.path(), &api.origin);
    let output = managed_cli(temp.path(), &api.origin)
        .args(["cancel", "delivery-one", "--json"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        parse_stdout_json(&output)["requested_deployment_id"],
        "delivery-one"
    );
    api.assert_complete();
    assert_no_core_effects(temp.path());
}

#[test]
fn managed_operation_rejects_retired_organization_override_before_authentication() {
    let dir = tempdir().unwrap();
    let api = lifecycle_fixture(vec![]);
    write_managed_target(dir.path(), &api.origin);
    let output = output_failure(managed_cli(dir.path(), &api.origin).args([
        "operation",
        "operation-one",
        "--organization",
        "foreign",
        "--json",
    ]));
    assert_eq!(output.status.code(), Some(64));
    assert!(String::from_utf8_lossy(&output.stderr).contains("unexpected argument"));
    assert!(api.requests().is_empty());
}

#[test]
fn managed_native_deployment_commands_refuse_a_foreign_organization_before_work() {
    for arguments in [
        vec!["plan", "--rev", MANAGED_REVISION],
        vec!["apply", "--plan", "saved-plan"],
        vec!["status", "delivery-one"],
        vec!["history"],
        vec!["cancel", "delivery-one"],
    ] {
        let temp = tempdir().unwrap();
        let api = IntentApiFixture::with_session(
            vec![],
            lifecycle_session("principal-one", "account-other"),
        );
        write_managed_target(temp.path(), &api.origin);
        let before = fs::read(managed_home(temp.path()).join("config.yaml")).unwrap();
        let output = managed_cli(temp.path(), &api.origin)
            .args(arguments)
            .arg("--json")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{output:?}");
        assert_eq!(parse_stdout_json(&output)["type"], "organization_mismatch");
        let requests = api.requests();
        assert_eq!(requests.len(), 1);
        assert_control_request(&requests[0], "GET", "/v1/auth/session", Value::Null, None);
        assert!(api.workflow_requests().is_empty());
        assert_eq!(
            fs::read(managed_home(temp.path()).join("config.yaml")).unwrap(),
            before
        );
        api.assert_complete();
        assert_no_core_effects(temp.path());
    }
}

fn organization_cli(root: &std::path::Path, origin: &str) -> assert_cmd::Command {
    let mut command = managed_fixture_command(root, origin, "managed-account");
    command.arg("organization");
    command
}

fn team_scopes() -> Value {
    serde_json::json!({"admin":false,"clusters":[],"verbs":[],"control_grants":[],"data_grants":[],"cluster_admissions":[]})
}

fn organization_members() -> Value {
    serde_json::json!({"data":{"organization_id":"account-one","members":[
        {"user_id":"user_one","membership_id":"om_one","membership_status":"active","principal_id":"principal-one","grant_version":3,"scopes":team_scopes(),"name":"Avery Chen","email":"avery@example.com"},
        {"user_id":"user_two","membership_id":"om_two","membership_status":"pending","principal_id":null,"grant_version":0,"scopes":team_scopes(),"name":null,"email":null}
    ]},"meta":{"provenance":"service_db"}})
}

fn team_invitation(state: &str) -> Value {
    serde_json::json!({"id":"inv_one","email":"avery@example.com","state":state,"organization_id":"account-one","accepted_user_id":null,"created_at":"2026-10-15T00:00:00Z","expires_at":"2026-10-22T00:00:00Z"})
}

fn team_patch(action: &str, version: i64) -> Value {
    let mut patch = serde_json::json!({"version":1,"organization_id":"account-one","user_id":"user_one","action":action,"expected_grant_version":version,"add":{"control_grants":[],"cluster_admissions":[]},"remove":{"control_grants":[],"cluster_admissions":[]}});
    if action != "remove" {
        patch["add"] = serde_json::json!({"control_grants":[{"cluster":"managed-test","verb":"status"},{"cluster":null,"verb":"members_manage"}],"cluster_admissions":["managed-test"]});
    }
    patch
}

fn team_patch_receipt(action: &str, version: i64) -> Value {
    let mut scopes = team_scopes();
    let patch = team_patch(action, version);
    scopes["control_grants"] = patch["add"]["control_grants"].clone();
    scopes["cluster_admissions"] = patch["add"]["cluster_admissions"].clone();
    serde_json::json!({"data":{"organization_id":"account-one","action":action,"previous_grant_version":version,"member":{"user_id":"user_one","principal_id":"principal-one","grant_version":version+1,"scopes":scopes},"provider_membership_verified":action!="remove","provider_changed":false,"graph_policy_changed":false,"maximum_existing_native_credential_lifetime_seconds":86400},"meta":{"provenance":"service_db"}})
}

#[test]
fn managed_organization_lists_directory_and_invitation_pages_without_enrolling() {
    for json in [true, false] {
        let temp = tempdir().unwrap();
        let response = organization_members();
        let api = lifecycle_fixture(vec![IntentReply::json(200, response.clone())]);
        let mut command = organization_cli(temp.path(), &api.origin);
        command.args(["members", "list"]);
        if json {
            command.arg("--json");
        }
        let output = output_success(&mut command);
        if json {
            assert_eq!(parse_stdout_json(&output), response);
        } else {
            let text = String::from_utf8_lossy(&output.stdout);
            for expected in [
                "Avery Chen",
                "avery@example.com",
                "user_two",
                "pending",
                "not enrolled",
                "grant version: 3",
                "Cedar",
            ] {
                assert!(text.contains(expected), "{text}");
            }
        }
        assert_control_request(
            &api.workflow_requests()[0],
            "GET",
            "/v1/organization/members",
            Value::Null,
            None,
        );
        assert!(!temp.path().join(".omnigraph").exists());
        api.assert_complete();
    }
    let temp = tempdir().unwrap();
    let response = serde_json::json!({"data":{"organization_id":"account-one","invitations":[team_invitation("pending")],"after":"cursor-next"},"meta":{}});
    let api = lifecycle_fixture(vec![IntentReply::json(200, response.clone())]);
    write_managed_target(temp.path(), &api.origin);
    let output = output_success(
        managed_fixture_command(temp.path(), &api.origin, "managed-account")
            .current_dir(temp.path())
            .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
            .env("OMNIGRAPH_CONTROL_API", &api.origin)
            .args([
                "org",
                "invitations",
                "list",
                "--after",
                "cursor/a+b",
                "--json",
            ]),
    );
    assert_eq!(parse_stdout_json(&output), response);
    assert_control_request(
        &api.workflow_requests()[0],
        "GET",
        "/v1/organization/invitations?after=cursor%2Fa%2Bb",
        Value::Null,
        None,
    );
    api.assert_complete();
}

#[test]
fn managed_organization_writes_send_exact_identity_and_required_key_once() {
    for action in ["send", "resend", "revoke"] {
        let temp = tempdir().unwrap();
        let response = serde_json::json!({"data":{"organization_id":"account-one","action":action,"invitation":team_invitation(if action=="revoke" {"revoked"} else {"pending"})},"meta":{}});
        let api = lifecycle_fixture(vec![IntentReply::json(200, response.clone())]);
        let output = output_success(organization_cli(temp.path(), &api.origin).args([
            "invitations",
            action,
            if action == "send" {
                "avery@example.com"
            } else {
                "inv_one"
            },
            "--idempotency-key",
            "invite-key",
            "--json",
        ]));
        assert_eq!(parse_stdout_json(&output), response);
        let (path, input) = if action == "send" {
            (
                "/v1/organization/invitations".to_owned(),
                serde_json::json!({"organization_id":"account-one","email":"avery@example.com"}),
            )
        } else {
            (
                format!("/v1/organization/invitations/inv_one:{action}"),
                serde_json::json!({"organization_id":"account-one"}),
            )
        };
        assert_control_request(
            &api.workflow_requests()[0],
            "POST",
            &path,
            input,
            Some("invite-key"),
        );
        api.assert_complete();
    }
    for (action, version, unchanged) in [
        ("add", 0, false),
        ("update", 3, false),
        ("remove", 3, false),
        ("update", 3, true),
        ("remove", 3, true),
    ] {
        let temp = tempdir().unwrap();
        let input = team_patch(action, version);
        fs::write(
            temp.path().join("member.yaml"),
            yaml_serde::to_string(&input).unwrap(),
        )
        .unwrap();
        let mut response = team_patch_receipt(action, version);
        if unchanged {
            response["data"]["member"]["grant_version"] = serde_json::json!(version);
        }
        let api = lifecycle_fixture(vec![IntentReply::json(200, response.clone())]);
        let output = output_success(organization_cli(temp.path(), &api.origin).args([
            "members",
            "apply",
            "member.yaml",
            "--idempotency-key",
            "member-key",
            "--json",
        ]));
        assert_eq!(parse_stdout_json(&output), response);
        assert_control_request(
            &api.workflow_requests()[0],
            "POST",
            "/v1/organization/members",
            input,
            Some("member-key"),
        );
        api.assert_complete();
    }
}

#[test]
fn managed_organization_offboard_preserves_partial_outcome_and_graph_boundary() {
    for (provider, session_state, revoked, exit) in [
        ("removed", "complete", 2, 0),
        ("uncertain", "partial", 1, 3),
    ] {
        let temp = tempdir().unwrap();
        let response = serde_json::json!({"data":{"organization_id":"account-one","user_id":"user_one","principal_id":"principal-one","grant_version":4,"local_access_removed":true,"provider_membership":provider,"sessions":{"revoked":revoked,"total":2,"state":session_state},"graph_policy_changed":false,"maximum_existing_native_credential_lifetime_seconds":86400},"meta":{}});
        let api = lifecycle_fixture(vec![IntentReply::json(200, response.clone())]);
        let output = organization_cli(temp.path(), &api.origin)
            .args([
                "members",
                "offboard",
                "user_one",
                "--expected-version",
                "3",
                "--idempotency-key",
                "offboard-key",
                "--json",
            ])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(exit));
        assert_eq!(parse_stdout_json(&output), response);
        assert_control_request(
            &api.workflow_requests()[0],
            "POST",
            "/v1/organization/members/user_one:offboard",
            serde_json::json!({"version":1,"organization_id":"account-one","user_id":"user_one","expected_grant_version":3}),
            Some("offboard-key"),
        );
        api.assert_complete();
    }
}

#[test]
fn managed_organization_refuses_invalid_targets_and_yaml_before_external_access() {
    let temp = tempdir().unwrap();
    let api = lifecycle_fixture(vec![]);
    for arguments in [
        vec![
            "invitations",
            "send",
            "bad\n@example.com",
            "--idempotency-key",
            "key",
        ],
        vec![
            "invitations",
            "revoke",
            "../inv",
            "--idempotency-key",
            "key",
        ],
        vec!["invitations", "list", "--after", "bad\n"],
        vec![
            "members",
            "offboard",
            "../user",
            "--expected-version",
            "0",
            "--idempotency-key",
            "key",
        ],
        vec![
            "invitations",
            "send",
            "avery@example.com",
            "--idempotency-key",
            "space key",
        ],
    ] {
        let output = organization_cli(temp.path(), &api.origin)
            .args(arguments)
            .arg("--json")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{output:?}");
    }
    for (pointer, value) in [
        ("/organization_id", serde_json::json!("foreign")),
        ("/version", serde_json::json!(2)),
        ("/expected_grant_version", serde_json::json!(1)),
        (
            "/add/control_grants",
            serde_json::json!([{"cluster":"*","verb":"status"}]),
        ),
        (
            "/add/control_grants",
            serde_json::json!([{"cluster":null,"verb":"plan"}]),
        ),
        (
            "/add/cluster_admissions",
            serde_json::json!(["cluster", "cluster"]),
        ),
    ] {
        let mut patch = team_patch("add", 0);
        *patch.pointer_mut(pointer).unwrap() = value;
        fs::write(
            temp.path().join("member.yaml"),
            yaml_serde::to_string(&patch).unwrap(),
        )
        .unwrap();
        let output = organization_cli(temp.path(), &api.origin)
            .args([
                "members",
                "apply",
                "member.yaml",
                "--idempotency-key",
                "key",
                "--json",
            ])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{output:?}");
    }
    fs::write(
        temp.path().join("member.yaml"),
        format!(
            "{}\nadmin: true\n",
            yaml_serde::to_string(&team_patch("add", 0)).unwrap()
        ),
    )
    .unwrap();
    let output = organization_cli(temp.path(), &api.origin)
        .args([
            "members",
            "apply",
            "member.yaml",
            "--idempotency-key",
            "key",
            "--json",
        ])
        .output()
        .unwrap();
    assert_eq!(parse_stdout_json(&output)["type"], "member_patch_invalid");
    fs::write(
        temp.path().join("oversized.yaml"),
        vec![b'a'; 256 * 1024 + 1],
    )
    .unwrap();
    let output = organization_cli(temp.path(), &api.origin)
        .args([
            "members",
            "apply",
            "oversized.yaml",
            "--idempotency-key",
            "key",
            "--json",
        ])
        .output()
        .unwrap();
    assert_eq!(parse_stdout_json(&output)["type"], "member_patch_invalid");
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("member.yaml", temp.path().join("linked.yaml")).unwrap();
        let output = organization_cli(temp.path(), &api.origin)
            .args([
                "members",
                "apply",
                "linked.yaml",
                "--idempotency-key",
                "key",
                "--json",
            ])
            .output()
            .unwrap();
        assert_eq!(parse_stdout_json(&output)["type"], "member_patch_invalid");
    }
    write_managed_target(temp.path(), &api.origin);
    for argument in ["--direct", "--target"] {
        let mut command = organization_cli(temp.path(), &api.origin);
        command.args(["members", "list", "--json", argument]);
        if argument == "--target" {
            command.arg("https://other.example");
        }
        let output = command.output().unwrap();
        let (exit, diagnostic) = if argument == "--direct" {
            (64, "unexpected argument '--direct'")
        } else {
            (1, "this operation selects --profile, not --target")
        };
        assert_eq!(output.status.code(), Some(exit), "{output:?}");
        assert!(output.stdout.is_empty());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(diagnostic),
            "{output:?}"
        );
    }
    let output = organization_cli(temp.path(), &api.origin)
        .args(["members", "list", "--organization", "foreign", "--json"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(api.requests().is_empty());
    assert_no_core_effects(temp.path());
    api.assert_complete();
}

#[test]
fn managed_organization_refuses_foreign_malformed_and_uncertain_results_without_replay() {
    for (pointer, value) in [
        ("/data/member/user_id", serde_json::json!("user_foreign")),
        ("/data/previous_grant_version", serde_json::json!(10)),
        ("/data/member/scopes/control_grants", serde_json::json!([])),
        ("/data/graph_policy_changed", serde_json::json!(true)),
    ] {
        let temp = tempdir().unwrap();
        fs::write(
            temp.path().join("member.yaml"),
            yaml_serde::to_string(&team_patch("add", 0)).unwrap(),
        )
        .unwrap();
        let mut response = team_patch_receipt("add", 0);
        *response.pointer_mut(pointer).unwrap() = value;
        let api = lifecycle_fixture(vec![IntentReply::json(200, response)]);
        let output = organization_cli(temp.path(), &api.origin)
            .args([
                "members",
                "apply",
                "member.yaml",
                "--idempotency-key",
                "member-key",
                "--json",
            ])
            .output()
            .unwrap();
        assert_eq!(parse_stdout_json(&output)["type"], "api_response_invalid");
        assert_eq!(parse_stdout_json(&output)["idempotency_key"], "member-key");
        assert_eq!(api.workflow_requests().len(), 1);
        api.assert_complete();
    }
    for (pointer, value) in [
        ("/data/organization_id", serde_json::json!("foreign")),
        ("/data/members/0/name", serde_json::json!("bad\u{1b}[2J")),
        (
            "/data/members/1/principal_id",
            serde_json::json!("principal-two"),
        ),
        (
            "/data/members/0/membership_status",
            serde_json::json!("unknown"),
        ),
        ("/data/members/1/user_id", serde_json::json!("user_one")),
    ] {
        let temp = tempdir().unwrap();
        let mut response = organization_members();
        *response.pointer_mut(pointer).unwrap() = value;
        let api = lifecycle_fixture(vec![IntentReply::json(200, response)]);
        let output = organization_cli(temp.path(), &api.origin)
            .args(["members", "list", "--json"])
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(parse_stdout_json(&output).get("data").is_none());
        assert_eq!(api.workflow_requests().len(), 1);
        api.assert_complete();
    }
    for reply in [
        IntentReply::json(
            403,
            serde_json::json!({"type":"scope_missing","detail":"not permitted"}),
        ),
        IntentReply::json(
            409,
            serde_json::json!({"type":"team_operation_uncertain","detail":"retain original request"}),
        ),
        IntentReply {
            status: 200,
            headers: vec![],
            body: b"{\"data\":".to_vec(),
        },
        IntentReply::json(
            200,
            serde_json::json!({"data":{"organization_id":"account-one","action":"send","invitation":{"token":"must-never-print"}},"meta":{}}),
        ),
    ] {
        let temp = tempdir().unwrap();
        let api = lifecycle_fixture(vec![reply]);
        let output = organization_cli(temp.path(), &api.origin)
            .args([
                "invitations",
                "send",
                "avery@example.com",
                "--idempotency-key",
                "same-key",
                "--json",
            ])
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert_eq!(parse_stdout_json(&output)["idempotency_key"], "same-key");
        assert!(!String::from_utf8_lossy(&output.stdout).contains("must-never-print"));
        assert_eq!(api.workflow_requests().len(), 1);
        api.assert_complete();
    }
}

#[test]
fn managed_organization_requires_an_account_target_and_write_keys() {
    let temp = tempdir().unwrap();
    let api = lifecycle_fixture(vec![]);
    write_managed_target(temp.path(), &api.origin);
    let missing = output_failure(cli().args(["organization", "members", "list"]));
    assert!(String::from_utf8_lossy(&missing.stderr).contains("profile_required"));
    for arguments in [
        vec!["invitations", "send", "member@example.com"],
        vec!["invitations", "resend", "inv_one"],
        vec!["invitations", "revoke", "inv_one"],
        vec!["members", "offboard", "user_one", "--expected-version", "1"],
    ] {
        let output = output_failure(organization_cli(temp.path(), &api.origin).args(arguments));
        assert!(String::from_utf8_lossy(&output.stderr).contains("--idempotency-key"));
    }
    assert!(api.requests().is_empty());
}
