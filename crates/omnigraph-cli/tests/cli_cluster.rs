//! Direct, served, and explicitly selected managed cluster command workflows.

use serde_json::Value;
use std::fs;

use tempfile::tempdir;

mod support;

use support::managed_http::{IntentApiFixture, IntentReply, IntentRequest};
use support::*;

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

/// The source directory belongs to the caller; the file storage root belongs
/// to the server. An absent client-side mount must not prevent HTTP submission.
#[test]
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
        let output = output_failure(
            cli()
                .env("OMNIGRAPH_BEARER_TOKEN", "capture-token")
                .args([
                    "cluster",
                    "apply",
                    "--server",
                    &api.origin,
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
            .args([
                "cluster",
                "apply",
                "--server",
                &api.origin,
                "--deployment-id",
                id,
                "--config",
            ])
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
            .args([
                "cluster",
                "apply",
                "--server",
                &api.origin,
                "--deployment-id",
                id,
                "--config",
            ])
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
    let observed = output_success(cli().args([
        "cluster",
        "status",
        "--server",
        &api.origin,
        "--deployment-id",
        id,
        "--wait",
        "--timeout",
        "10",
        "--json",
    ]));
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
        command.timeout(std::time::Duration::from_secs(15)).args([
            "cluster",
            if apply { "apply" } else { "status" },
            "--server",
            &api.origin,
            "--deployment-id",
            id,
            "--timeout",
            "10",
            "--json",
        ]);
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
        let output = output_failure(cli().args([
            "cluster",
            "status",
            "--server",
            &api.origin,
            "--deployment-id",
            id,
            "--wait",
            "--timeout",
            "10",
            "--json",
        ]));
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
        let output = output_failure(cli().args([
            "cluster",
            "status",
            "--server",
            &api.origin,
            "--deployment-id",
            id,
            "--wait",
            "--json",
        ]));
        assert_eq!(parse_stdout_json(&output), response);
        api.assert_complete();
    }

    // A different declared root still refuses before POST; the server's status
    // is binding evidence, never permission to silently retarget the bundle.
    fs::write(&config, format!("storage: {remote_root}-other\n{source}")).unwrap();
    let api = IntentApiFixture::graph(vec![IntentReply::json(200, status)]);
    output_failure(
        cli()
            .env("OMNIGRAPH_BEARER_TOKEN", "capture-token")
            .args(["cluster", "apply", "--server", &api.origin, "--config"])
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
            let mut command = cli();
            command
                .current_dir(temp.path())
                .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
                .env("OMNIGRAPH_CONTROL_API", &api.origin)
                .args([
                    "cluster",
                    "create",
                    "--managed",
                    name,
                    "--api",
                    &api.origin,
                    "--no-wait",
                    "--json",
                ]);
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
        let pending_path = temp.path().join(".omnigraph/pending-lifecycle.json");
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
            &fs::read(temp.path().join(".omnigraph/last-lifecycle.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(saved["operation_id"], "operation-one");
        let context = fs::read(temp.path().join(".omnigraph/context")).unwrap();
        assert_eq!(invoke("another-cluster", None).status.code(), Some(2));
        assert_eq!(
            fs::read(temp.path().join(".omnigraph/context")).unwrap(),
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
        cli()
            .current_dir(temp.path())
            .env("OMNIGRAPH_CONTROL_TOKEN", token)
            .env("OMNIGRAPH_CONTROL_API", &api.origin)
            .args([
                "cluster",
                "create",
                "--managed",
                "new-name",
                "--api",
                &api.origin,
                "--no-wait",
                "--json",
            ])
            .output()
            .unwrap()
    };
    assert_eq!(invoke("og_fixture_control").status.code(), Some(1));
    let path = temp.path().join(".omnigraph/pending-lifecycle.json");
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
            "pending_lifecycle_conflict"
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
        cli()
            .current_dir(temp.path())
            .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
            .env("OMNIGRAPH_CONTROL_API", &api.origin)
            .args([
                "cluster",
                "create",
                "--managed",
                name,
                "--api",
                &api.origin,
                "--no-wait",
                "--json",
            ])
            .output()
            .unwrap()
    };
    assert_eq!(invoke("taken-name").status.code(), Some(2));
    assert!(
        !temp
            .path()
            .join(".omnigraph/pending-lifecycle.json")
            .exists()
    );
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
        write_managed_context(temp.path(), &api.origin);
        let before = fs::read(temp.path().join(".omnigraph/context")).unwrap();
        let output = output_success(
            cli()
                .current_dir(temp.path())
                .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
                .env("OMNIGRAPH_CONTROL_API", &api.origin)
                .arg("cluster")
                .args(args)
                .arg("--managed")
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
            fs::read(temp.path().join(".omnigraph/context")).unwrap(),
            before
        );
        api.assert_complete();
        assert_no_core_effects(temp.path());
    }
}

#[test]
fn managed_lifecycle_wait_reports_tombstone_and_checks_every_poll_identity() {
    let temp = tempdir().unwrap();
    let tombstone = lifecycle_envelope("delete", "running", "tombstoned");
    let api = lifecycle_fixture(vec![
        IntentReply::json(202, lifecycle_envelope("delete", "proposed", "requested")),
        IntentReply::json(200, tombstone.clone()),
    ]);
    write_managed_context(temp.path(), &api.origin);
    let output = output_success(
        cli()
            .current_dir(temp.path())
            .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
            .env("OMNIGRAPH_CONTROL_API", &api.origin)
            .args([
                "cluster",
                "delete",
                "--managed",
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
    let output = cli()
        .current_dir(temp.path())
        .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
        .env("OMNIGRAPH_CONTROL_API", &api.origin)
        .args([
            "cluster",
            "operation",
            "--managed",
            "operation-one",
            "--api",
            &api.origin,
            "--wait",
            "--timeout",
            "10",
            "--json",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(parse_stdout_json(&output)["type"], "context_mismatch");
    assert!(!temp.path().join(".omnigraph/context").exists());
    api.assert_complete();
}

#[test]
fn managed_lifecycle_bad_acceptance_and_deadline_keep_recovery_identity() {
    let temp = tempdir().unwrap();
    let mut bad = lifecycle_envelope("create", "proposed", "provisioning");
    bad["meta"]["cluster_id"] = serde_json::json!("foreign");
    let api = lifecycle_fixture(vec![IntentReply::json(202, bad)]);
    let output = cli()
        .current_dir(temp.path())
        .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
        .env("OMNIGRAPH_CONTROL_API", &api.origin)
        .args([
            "cluster",
            "create",
            "--managed",
            "new",
            "--api",
            &api.origin,
            "--no-wait",
            "--json",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(
        temp.path()
            .join(".omnigraph/pending-lifecycle.json")
            .exists()
    );
    assert!(!temp.path().join(".omnigraph/context").exists());
    api.assert_complete();

    let temp = tempdir().unwrap();
    let accepted = lifecycle_envelope("create", "proposed", "provisioning");
    let api = lifecycle_fixture(vec![IntentReply::json(202, accepted.clone())]);
    let output = cli()
        .current_dir(temp.path())
        .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
        .env("OMNIGRAPH_CONTROL_API", &api.origin)
        .args([
            "cluster",
            "create",
            "--managed",
            "new",
            "--api",
            &api.origin,
            "--timeout",
            "1",
            "--json",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(5));
    assert_eq!(parse_stdout_json(&output), accepted);
    assert!(temp.path().join(".omnigraph/last-lifecycle.json").exists());
    assert_eq!(api.workflow_requests().len(), 1);
    api.assert_complete();
    assert_no_core_effects(temp.path());
}

#[test]
#[cfg(unix)]
fn managed_lifecycle_push_sends_only_complete_referenced_files() {
    let temp = tempdir().unwrap();
    let response = serde_json::json!({"data":{"cluster_id":"managed-test","revision":"a".repeat(40)},"meta":{"cluster_id":"managed-test","incarnation":"inc-one"}});
    let api = lifecycle_fixture(vec![IntentReply::json(200, response.clone())]);
    write_managed_context(temp.path(), &api.origin);
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
        cli()
            .current_dir(temp.path())
            .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
            .env("OMNIGRAPH_CONTROL_API", &api.origin)
            .args([
                "cluster",
                "push",
                "--managed",
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
    write_managed_context(temp.path(), &api.origin);
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
        let output = cli()
            .current_dir(temp.path())
            .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
            .env("OMNIGRAPH_CONTROL_API", &api.origin)
            .args([
                "cluster",
                "push",
                "--managed",
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
    fs::create_dir(temp.path().join(".omnigraph")).unwrap();
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(temp.path().join(".omnigraph/lifecycle.lock"))
        .unwrap();
    lock.try_lock().unwrap();
    let output = cli()
        .current_dir(temp.path())
        .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
        .env("OMNIGRAPH_CONTROL_API", &api.origin)
        .args([
            "cluster",
            "create",
            "--managed",
            "locked",
            "--api",
            &api.origin,
            "--no-wait",
            "--json",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(
        parse_stdout_json(&output)["type"],
        "lifecycle_submission_busy"
    );
    assert!(
        !temp
            .path()
            .join(".omnigraph/pending-lifecycle.json")
            .exists()
    );
    drop(lock);
    for args in [
        vec![
            "cluster",
            "create",
            "--managed",
            "direct",
            "--api",
            &api.origin,
            "--direct",
        ],
        vec![
            "cluster",
            "delete",
            "--managed",
            "--incarnation",
            "inc-one",
            "--direct",
        ],
        vec![
            "cluster",
            "undo-delete",
            "--managed",
            "--incarnation",
            "inc-one",
            "--deletion-id",
            "op",
            "--direct",
        ],
        vec![
            "cluster",
            "operation",
            "--managed",
            "op",
            "--api",
            &api.origin,
            "--direct",
        ],
    ] {
        let output = cli()
            .current_dir(temp.path())
            .args(args)
            .arg("--json")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert_eq!(parse_stdout_json(&output)["type"], "managed_scope_conflict");
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
    write_managed_context(temp.path(), &api.origin);
    fs::write(outside.path().join("private.pg"), "DO_NOT_SEND").unwrap();
    std::os::unix::fs::symlink(outside.path(), temp.path().join("linked")).unwrap();
    fs::write(
        temp.path().join("cluster.yaml"),
        "version: 1\ngraphs:\n  sample:\n    schema: linked/private.pg\n",
    )
    .unwrap();
    let output = cli()
        .current_dir(temp.path())
        .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
        .env("OMNIGRAPH_CONTROL_API", &api.origin)
        .args([
            "cluster",
            "push",
            "--managed",
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
    std::os::unix::fs::symlink(
        outside.path().join("private.pg"),
        temp.path().join(".omnigraph/pending-lifecycle.json"),
    )
    .unwrap();
    let output = cli()
        .current_dir(temp.path())
        .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
        .env("OMNIGRAPH_CONTROL_API", &api.origin)
        .args([
            "cluster",
            "delete",
            "--managed",
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
    write_managed_context(temp.path(), &api.origin);
    for args in [
        vec!["query", "q", "--json"],
        vec![
            "mutate",
            "m",
            "--graph",
            "knowledge",
            "--as",
            "forged",
            "--json",
        ],
        vec![
            "cluster",
            "token",
            "--managed",
            "--clear",
            "--graph",
            "knowledge",
            "--json",
        ],
    ] {
        let output = cli()
            .current_dir(temp.path())
            .env_remove("OMNIGRAPH_PROFILE")
            .args(&args)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{args:?}: {output:?}");
        let problem = parse_stdout_json(&output);
        assert!(
            matches!(
                problem["type"].as_str(),
                Some("graph_required" | "managed_scope_conflict" | "token_clear_conflict")
            ),
            "{problem}"
        );
        assert_no_core_effects(temp.path());
    }
    assert!(api.requests().is_empty());
    assert_no_core_effects(temp.path());
}

#[test]
fn managed_data_direct_override_uses_only_explicit_legacy_transport() {
    let temp = tempdir().unwrap();
    let api = IntentApiFixture::new(vec![]);
    write_managed_context(temp.path(), &api.origin);
    fs::write(temp.path().join(".omnigraph/context"), "malformed").unwrap();
    let data = IntentApiFixture::graph(vec![IntentReply::json(
        200,
        serde_json::json!({
            "query_name":"q", "target":{"branch":"main"}, "row_count":1,
            "columns":["value"], "rows":[{"value":42}], "graph_commit_id":"head-a"
        }),
    )]);
    let output = output_success(
        cli()
            .current_dir(temp.path())
            .env("OMNIGRAPH_BEARER_TOKEN", "explicit-legacy-token")
            .env("OMNIGRAPH_CONTROL_TOKEN", "never-data")
            .env("OMNIGRAPH_CONTROL_API", &api.origin)
            .args(["query", "q", "--graph", "knowledge", "--server"])
            .arg(&data.origin)
            .args(["--json", "--direct"]),
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
    for malformed in [false, true] {
        for selector in ["--server", "--profile"] {
            for verb in ["query", "mutate", "load"] {
                let temp = tempdir().unwrap();
                let api = IntentApiFixture::new(vec![]);
                write_managed_context(temp.path(), &api.origin);
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
                        "servers:\n  staging:\n    url: {}\nprofiles:\n  staging:\n    server: staging\n    default_graph: knowledge\n",
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
                if selector == "--server" {
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
fn managed_data_issue_633_ambient_targets_refuse_without_selecting_either() {
    for (config, profile) in [
        ("{}", Some("unknown-profile")),
        ("defaults:\n  server: staging\n", None),
        ("defaults:\n  store: file:///must-not-open\n", None),
    ] {
        let temp = tempdir().unwrap();
        let api = IntentApiFixture::new(vec![]);
        write_managed_context(temp.path(), &api.origin);
        let home = temp.path().join("operator");
        fs::create_dir(&home).unwrap();
        fs::write(home.join("config.yaml"), config).unwrap();
        for verb in ["query", "mutate", "load"] {
            let mut command = cli();
            command
                .current_dir(temp.path())
                .env("OMNIGRAPH_HOME", &home)
                .env_remove("OMNIGRAPH_PROFILE")
                .env("OMNIGRAPH_BEARER_TOKEN", "must-not-be-used")
                .args([verb, "--json"])
                .timeout(std::time::Duration::from_secs(15));
            if verb == "load" {
                command.args(["--data", "missing.jsonl", "--mode", "append"]);
            } else {
                command.arg("q");
            }
            if let Some(profile) = profile {
                command.env("OMNIGRAPH_PROFILE", profile);
            }
            // No graph means the old dispatcher fails without touching a real
            // keychain too; the regression is which target decision occurs first.
            let output = command.output().unwrap();
            assert_eq!(output.status.code(), Some(2));
            assert_eq!(
                parse_stdout_json(&output)["type"],
                "managed_target_ambiguous"
            );
            assert!(api.requests().is_empty());
            assert_no_core_effects(temp.path());
        }
    }
}

#[test]
fn managed_data_issue_633_direct_load_and_commit_preserve_ambient_targets() {
    for malformed in [false, true] {
        for use_profile in [false, true] {
            for operation in ["load", "commit-list", "commit-show"] {
                let temp = tempdir().unwrap();
                let api = IntentApiFixture::new(vec![]);
                write_managed_context(temp.path(), &api.origin);
                if malformed {
                    fs::write(temp.path().join(".omnigraph/context"), "malformed").unwrap();
                }
                let commit = serde_json::json!({
                    "graph_commit_id":"commit-a", "graph_branch":null,
                    "graph_manifest_version":3, "parent_commit_id":"prior",
                    "merged_parent_commit_id":null, "actor_id":"legacy-actor",
                    "created_at":123456,
                });
                let reply = match operation {
                    "load" => serde_json::json!({
                        "branch":"main", "base_branch":null, "branch_created":false,
                        "mode":"append", "nodes":[{"name":"Person", "entities_loaded":1}],
                        "edges":[], "total_entities":1, "actor_id":"legacy-actor", "commit":null,
                        "embedding_generation":null
                    }),
                    "commit-list" => serde_json::json!({"commits":[commit.clone()]}),
                    _ => commit.clone(),
                };
                let data = IntentApiFixture::graph(vec![IntentReply::json(200, reply)]);
                let home = temp.path().join("operator");
                fs::create_dir(&home).unwrap();
                fs::write(
                    home.join("config.yaml"),
                    format!(
                        "servers:\n  staging:\n    url: {}\nprofiles:\n  staging:\n    server: staging\n    default_graph: knowledge\ndefaults:\n  server: staging\n  default_graph: knowledge\n",
                        data.origin
                    ),
                )
                .unwrap();
                let mut command = cli();
                command
                    .current_dir(temp.path())
                    .env("OMNIGRAPH_HOME", &home)
                    .env_remove("OMNIGRAPH_PROFILE")
                    .env("OMNIGRAPH_TOKEN_STAGING", "ordinary-ambient-token")
                    .env("OMNIGRAPH_BEARER_TOKEN", "unused-legacy-fallback")
                    .timeout(std::time::Duration::from_secs(15));
                if use_profile {
                    command.env("OMNIGRAPH_PROFILE", "staging");
                }
                match operation {
                    "load" => {
                        fs::write(temp.path().join("batch.jsonl"), "{}\n").unwrap();
                        command.args(["load", "--data", "batch.jsonl", "--mode", "append"]);
                    }
                    "commit-list" => {
                        command.args(["commit", "list"]);
                    }
                    _ => {
                        command.args(["commit", "show", "commit-a"]);
                    }
                }
                command.args(["--direct", "--json"]);
                let output = output_success(&mut command);
                let payload = parse_stdout_json(&output);
                match operation {
                    "load" => assert_eq!(payload["total_entities"], 1),
                    "commit-list" => assert_eq!(payload["commits"][0], commit),
                    _ => assert_eq!(payload, commit),
                }
                let requests = data.workflow_requests();
                assert_eq!(requests.len(), 1);
                assert_eq!(
                    requests[0].path,
                    match operation {
                        "load" => "/graphs/knowledge/load/ndjson?branch=main&mode=append",
                        "commit-list" => "/graphs/knowledge/commits",
                        _ => "/graphs/knowledge/commits/commit-a",
                    }
                );
                assert_eq!(
                    requests[0].headers["authorization"],
                    "Bearer ordinary-ambient-token"
                );
                assert!(api.requests().is_empty());
                data.assert_complete();
                assert_no_core_effects(temp.path());
            }
        }
    }
}

#[test]
fn managed_data_issue_633_folder_context_does_not_gate_local_graph_work() {
    let temp = tempdir().unwrap();
    let api = IntentApiFixture::new(vec![]);
    write_managed_context(temp.path(), &api.origin);
    fs::write(temp.path().join(".omnigraph/context"), "malformed").unwrap();
    let graph = graph_path(temp.path());
    let home = temp.path().join("operator");
    fs::create_dir(&home).unwrap();
    fs::write(
        home.join("config.yaml"),
        format!("defaults:\n  store: {}\n", graph.display()),
    )
    .unwrap();
    let schema = fixture("test.pg");
    let queries = fixture("test.gq");
    let command = || {
        let mut command = cli();
        command
            .current_dir(temp.path())
            .env("OMNIGRAPH_HOME", &home)
            .env_remove("OMNIGRAPH_PROFILE")
            .env_remove("OMNIGRAPH_BEARER_TOKEN")
            .timeout(std::time::Duration::from_secs(30));
        command
    };
    output_success(
        command()
            .arg("init")
            .arg("--schema")
            .arg(&schema)
            .arg(&graph),
    );
    assert!(graph.exists());
    output_success(
        command()
            .args(["load", "--mode", "append", "--data"])
            .arg(fixture("test.jsonl"))
            .arg("--store")
            .arg(&graph)
            .arg("--json"),
    );
    output_success(
        command()
            .args(["lint", "--schema"])
            .arg(&schema)
            .arg("--query")
            .arg(&queries)
            .arg("--json"),
    );
    let query = output_success(
        command()
            .args(["query", "get_person", "--query"])
            .arg(&queries)
            .arg("--store")
            .arg(&graph)
            .args(["--params", r#"{"name":"Alice"}"#, "--json"]),
    );
    assert_eq!(parse_stdout_json(&query)["rows"][0]["p.name"], "Alice");
    output_success(
        command()
            .args(["schema", "plan", "--schema"])
            .arg(&schema)
            .arg("--store")
            .arg(&graph)
            .arg("--json"),
    );
    output_success(
        command()
            .args(["commit", "list", "--store"])
            .arg(&graph)
            .arg("--json"),
    );
    assert!(api.requests().is_empty());
    assert_no_core_effects(temp.path());
}

#[test]
fn managed_data_issue_633_operator_preferences_do_not_select_a_target() {
    let temp = tempdir().unwrap();
    let api = IntentApiFixture::new(vec![]);
    write_managed_context(temp.path(), &api.origin);
    let home = temp.path().join("operator");
    fs::create_dir(&home).unwrap();
    fs::write(
        home.join("config.yaml"),
        "operator:\n  actor: preferred-actor\ndefaults:\n  output: json\n  default_graph: knowledge\nprofiles:\n  unused:\n    server: staging\n",
    )
    .unwrap();
    for profile in [None, Some("")] {
        let mut command = cli();
        command
            .current_dir(temp.path())
            .env("OMNIGRAPH_HOME", &home)
            .env_remove("OMNIGRAPH_PROFILE")
            .args(["query", "q", "--json"]);
        if let Some(profile) = profile {
            command.env("OMNIGRAPH_PROFILE", profile);
        }
        let output = command.output().unwrap();
        assert_eq!(parse_stdout_json(&output)["type"], "graph_required");
        assert!(api.requests().is_empty());
    }
    fs::write(home.join("config.yaml"), "defaults: [invalid]").unwrap();
    let output = cli()
        .current_dir(temp.path())
        .env("OMNIGRAPH_HOME", &home)
        .env_remove("OMNIGRAPH_PROFILE")
        .args(["query", "q", "--json"])
        .output()
        .unwrap();
    assert_eq!(
        parse_stdout_json(&output)["type"],
        "operator_config_invalid"
    );
    assert!(api.requests().is_empty());
    assert_no_core_effects(temp.path());
}

#[test]
fn managed_data_issue_633_direct_preserves_ambient_legacy_resolution() {
    for use_profile in [false, true] {
        let temp = tempdir().unwrap();
        let api = IntentApiFixture::new(vec![]);
        write_managed_context(temp.path(), &api.origin);
        let data = IntentApiFixture::graph(vec![IntentReply::json(
            200,
            serde_json::json!({
                "query_name":"q", "target":{"branch":"main"}, "row_count":1,
                "columns":["value"], "rows":[{"value":42}], "graph_commit_id":"head-a"
            }),
        )]);
        let home = temp.path().join("operator");
        fs::create_dir(&home).unwrap();
        fs::write(
            home.join("config.yaml"),
            format!(
                "servers:\n  staging:\n    url: {}\nprofiles:\n  staging:\n    server: staging\ndefaults:\n  server: staging\n",
                data.origin
            ),
        )
        .unwrap();
        let mut command = cli();
        command
            .current_dir(temp.path())
            .env("OMNIGRAPH_HOME", &home)
            .env_remove("OMNIGRAPH_PROFILE")
            .env("OMNIGRAPH_TOKEN_STAGING", "legacy-ambient-token")
            .args(["query", "q", "--graph", "knowledge", "--json", "--direct"]);
        if use_profile {
            command.env("OMNIGRAPH_PROFILE", "staging");
        }
        let output = output_success(&mut command);
        assert_eq!(parse_stdout_json(&output)["rows"][0]["value"], 42);
        assert_eq!(
            data.workflow_requests()[0].headers["authorization"],
            "Bearer legacy-ambient-token"
        );
        assert!(api.requests().is_empty());
        data.assert_complete();
        assert_no_core_effects(temp.path());
    }
}

#[test]
fn managed_use_verifies_access_before_writing_context() {
    let temp = tempdir().unwrap();
    let body = serde_json::json!({"data":{"cluster_id":"managed-test","name":"prod"},"meta":{"cluster_id":"managed-test","assurance":"verified_workload"}});
    let status = serde_json::json!({"data":{"source":{"revision":MANAGED_REVISION},"requested":null,"observed":{"state":"observation_blocked"}},"meta":{"cluster_id":"managed-test","provenance":"service_db"}});
    let delivery = managed_complete(false, false, "archived");
    let history = serde_json::json!({"data":{"deployments":[delivery["data"].clone()]},"meta":{"cluster_id":"managed-test","provenance":"service_db"}});
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
    let replies = std::iter::once(IntentReply::json(200, body.clone()))
        .chain(
            reads
                .iter()
                .map(|(_, _, body, _)| IntentReply::json(200, body.clone())),
        )
        .collect();
    let api = IntentApiFixture::new(replies);
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
    assert_control_request(
        &api.requests()[0],
        "GET",
        "/v1/clusters/managed-test",
        Value::Null,
        None,
    );
    for (index, (args, path, response, refused)) in reads.into_iter().enumerate() {
        let output = managed_cli(temp.path(), &api.origin)
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
        assert_eq!(api.requests().len(), index + 2);
        assert_control_request(&api.requests()[index + 1], "GET", path, Value::Null, None);
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
        assert_control_request(
            &api.requests()[0],
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
        let source = serde_json::json!({"data":{"revision":MANAGED_REVISION},"meta":{"cluster_id":"managed-test"}});
        let expected = managed_preview(state);
        let api = IntentApiFixture::new(vec![
            IntentReply::json(200, source),
            IntentReply::json(200, expected.clone()),
        ]);
        write_managed_context(temp.path(), &api.origin);
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
            &api.requests()[0],
            "GET",
            "/v1/clusters/managed-test/config",
            Value::Null,
            None,
        );
        assert_control_request(
            &api.requests()[1],
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
        let api = IntentApiFixture::new(replies);
        write_managed_context(temp.path(), &api.origin);
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
                    .contains("cluster status --managed delivery-one")
            );
        }
        assert_control_request(
            &api.requests()[0],
            "POST",
            "/v1/clusters/managed-test/deployments",
            serde_json::json!({"preview_id":"saved-plan"}),
            Some("poll-key"),
        );
        if !timeout {
            assert_control_request(
                &api.requests()[1],
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
        let api = IntentApiFixture::new(vec![IntentReply::json(200, body.clone())]);
        write_managed_context(temp.path(), &api.origin);
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
            &api.requests()[0],
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
        let api = IntentApiFixture::new(vec![IntentReply::json(status, problem.clone())]);
        write_managed_context(temp.path(), &api.origin);
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
        let api = IntentApiFixture::new(vec![
            IntentReply::json(202, queued),
            IntentReply::json(200, changed),
        ]);
        write_managed_context(temp.path(), &api.origin);
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
            api.requests().iter().filter(|r| r.method == "POST").count(),
            1
        );
        api.assert_complete();
        assert_no_core_effects(temp.path());
    }
}

#[test]
fn managed_lost_submission_retains_key_without_effect_retry() {
    let temp = tempdir().unwrap();
    let api = IntentApiFixture::new(vec![IntentReply {
        status: 202,
        headers: vec![],
        body: b"{".to_vec(),
    }]);
    write_managed_context(temp.path(), &api.origin);
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
            .args(["apply", "--plan", "saved-plan", "--json"])
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
            .args(["apply", "--plan", "saved-plan", "--json"])
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
fn managed_mode_is_explicit_and_direct_commands_ignore_folder_context() {
    let temp = tempdir().unwrap();
    let api = IntentApiFixture::new(vec![]);
    write_managed_context(temp.path(), &api.origin);
    for context in ["valid", "malformed"] {
        let root = temp.path().join(context);
        fs::create_dir(&root).unwrap();
        write_cluster_config_fixture(&root);
        write_managed_context(&root, &api.origin);
        let context_path = root.join(".omnigraph/context");
        if context == "malformed" {
            fs::write(&context_path, "{\n").unwrap();
        }
        let original_context = fs::read(&context_path).unwrap();
        for verb in ["validate", "plan", "status"] {
            let output = output_success(cli().current_dir(&root).args(["cluster", verb, "--json"]));
            assert_eq!(parse_stdout_json(&output)["ok"], true, "{context}: {verb}");
        }
        let observed = cli()
            .current_dir(&root)
            .args(["cluster", "observe", "--json"])
            .output()
            .unwrap();
        assert!(!observed.status.success());
        let observed = parse_stdout_json(&observed);
        assert_eq!(observed["ok"], false);
        assert!(
            observed["diagnostics"]
                .as_array()
                .unwrap()
                .iter()
                .any(|diagnostic| diagnostic["code"] == "state_missing")
        );
        assert_no_core_effects(&root);
        let output = output_success(
            cli()
                .current_dir(&root)
                .env("OMNIGRAPH_CONTROL_API", &api.origin)
                .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
                .args(["cluster", "apply", "--json"]),
        );
        let receipt = parse_stdout_json(&output);
        assert_eq!(receipt["status"], "complete", "{context}: {receipt}");
        assert_eq!(receipt["result"]["converged"], true);
        assert_eq!(
            receipt["result"]["graphs"]["knowledge"]["outcome"],
            "created"
        );
        assert!(root.join("graphs/knowledge.omni/__manifest").exists());
        let ledger: Value =
            serde_json::from_slice(&fs::read(root.join("__cluster/state.json")).unwrap()).unwrap();
        assert_eq!(ledger["deployment_results"][0], receipt["result"]);
        assert_eq!(fs::read(&context_path).unwrap(), original_context);
        assert!(
            api.requests().is_empty(),
            "Core apply consulted the managed service"
        );
    }
    // Neither a selected folder nor an invalid context may change the meaning
    // of a command. Reject the spelling/flag conflict before reading context,
    // opening storage, or submitting anything to either HTTP API.
    let rejected_root = temp.path().join("rejected");
    fs::create_dir(&rejected_root).unwrap();
    write_cluster_config_fixture(&rejected_root);
    write_managed_context(&rejected_root, &api.origin);
    let rejected_context = rejected_root.join(".omnigraph/context");
    fs::write(&rejected_context, "{\n").unwrap();
    let mut rejected_arguments = vec![
        vec!["managed", "status"], // Removed spelling has no compatibility alias.
        vec!["cluster", "create", "new", "--api", &api.origin],
        vec![
            "cluster",
            "push",
            "--expected-revision",
            "old",
            "--message",
            "new",
        ],
        vec!["cluster", "delete", "--incarnation", "inc-one"],
        vec![
            "cluster",
            "undo-delete",
            "--incarnation",
            "inc-one",
            "--deletion-id",
            "op",
        ],
        vec!["cluster", "token"],
        vec!["cluster", "operation", "op"],
        vec!["cluster", "history"],
        vec!["cluster", "cancel", "run"],
        vec!["cluster", "plan", "--rev", "revision"],
        vec!["cluster", "plan", "--no-wait"],
        vec!["cluster", "plan", "--timeout", "10"],
        vec!["cluster", "plan", "--idempotency-key", "key"],
        vec!["cluster", "apply", "--plan", "plan"],
        vec!["cluster", "apply", "--idempotency-key", "key"],
        vec!["cluster", "status", "run"],
        vec!["cluster", "apply", "--managed"], // A managed apply requires a plan.
        vec![
            "cluster",
            "apply",
            "--managed",
            "--plan",
            "plan",
            "--deployment-id",
            "id",
        ],
        vec![
            "cluster",
            "apply",
            "--managed",
            "--plan",
            "plan",
            "--writers-stopped",
        ],
        vec!["cluster", "status", "--managed", "--deployment-id", "id"],
        vec!["cluster", "status", "--managed", "run", "--timeout", "10"],
        vec!["cluster", "validate", "--managed"],
        vec!["cluster", "observe", "--managed"],
        vec!["cluster", "force-unlock", "lock", "--managed"],
        vec![
            "cluster",
            "upgrade-ledger",
            "--writers-stopped",
            "--managed",
        ],
    ];
    for selector in [
        vec!["--server", api.origin.as_str()],
        vec!["--cluster", "file:///must-not-open"],
        vec!["--store", "file:///must-not-open"],
        vec!["--profile", "must-not-load"],
        vec!["--graph", "knowledge"],
        vec!["--as", "forged"],
        vec!["--direct"],
    ] {
        let mut arguments = vec!["cluster", "apply", "--managed", "--plan", "plan"];
        arguments.extend(selector);
        rejected_arguments.push(arguments);
    }
    for arguments in rejected_arguments {
        let output = cli()
            .current_dir(&rejected_root)
            .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
            .env("OMNIGRAPH_CONTROL_API", &api.origin)
            .args(&arguments)
            .arg("--json")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{arguments:?}: {output:?}");
        let diagnostics = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            !diagnostics.contains("context_invalid")
                && !diagnostics.contains("managed_context_required"),
            "argument refusal must precede context access: {arguments:?}: {diagnostics}"
        );
        assert!(api.requests().is_empty(), "{arguments:?}");
        assert_no_core_effects(&rejected_root);
        assert_eq!(fs::read(&rejected_context).unwrap(), b"{\n");
        assert!(!rejected_root.join(".omnigraph/lifecycle.lock").exists());
        assert!(
            !rejected_root
                .join(".omnigraph/pending-lifecycle.json")
                .exists()
        );
    }
    let child = temp.path().join("nested");
    fs::create_dir(&child).unwrap();
    let missing = managed_cli(&child, &api.origin)
        .args(["status", "--json"])
        .output()
        .unwrap();
    assert_eq!(missing.status.code(), Some(2));
    assert_eq!(
        parse_stdout_json(&missing)["type"],
        "managed_context_required"
    );
    assert!(api.requests().is_empty());
    assert_no_core_effects(&child);
}

#[test]
fn managed_cancel_uses_exact_cluster_scope_and_refuses_foreign_response() {
    let temp = tempdir().unwrap();
    let mut foreign = managed_delivery("cancelled");
    foreign["meta"]["cluster_id"] = serde_json::json!("another-cluster");
    let api = IntentApiFixture::new(vec![IntentReply::json(200, foreign)]);
    write_managed_context(temp.path(), &api.origin);
    let output = managed_cli(temp.path(), &api.origin)
        .args(["cancel", "delivery-one", "--json"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(parse_stdout_json(&output)["type"], "context_mismatch");
    assert_control_request(
        &api.requests()[0],
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
    let api = IntentApiFixture::new(vec![
        IntentReply::json(200, cancelled.clone()),
        IntentReply::json(200, cancelled.clone()),
        IntentReply::json(409, problem.clone()),
    ]);
    write_managed_context(temp.path(), &api.origin);
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
    for request in api.requests() {
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
fn cluster_plan_succeeds_before_and_after_force_unlock() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    write_cluster_lock(temp.path(), "held-lock", "plan");

    let locked = parse_stdout_json(&output_success(
        cli()
            .arg("cluster")
            .arg("plan")
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
fn cluster_apply_uses_operator_actor_from_omnigraph_home() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    let operator_home = tempdir().unwrap();
    fs::write(
        operator_home.path().join("config.yaml"),
        "operator:
  actor: act-operator
",
    )
    .unwrap();
    for (extra, expected) in [(vec![], "act-operator"), (vec!["--as", "andrew"], "andrew")] {
        let output = output_success(
            cli()
                .current_dir(temp.path())
                .env("OMNIGRAPH_HOME", operator_home.path())
                .args(extra)
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
            output.status.success() || command == "plan", // plan warns state-missing before bootstrap; still must not config-error
            "cluster {command} affected by malformed omnigraph.yaml: {output:?}"
        );
        assert!(
            !String::from_utf8_lossy(&output.stderr).contains("omnigraph.yaml"),
            "cluster {command} touched omnigraph.yaml"
        );
    }
    // Bootstrap apply (no --as, no operator config): the legacy file is never
    // loaded and the no-actor apply succeeds (actor defaults to none).
    let output = cli()
        .current_dir(temp.path())
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
        let output = output_failure(cli().args(&args).arg("--store").arg(&graph).arg("--yes"));
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
    let refused = output_failure(cli().args(["schema", "show"]).arg(&graph));
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
            args.extend(["--store", graph.to_str().unwrap()]);
        }
        reads.push(vec![
            "queries",
            "validate",
            "--cluster",
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
            .args(["branch", "create", "after-reads", "--store"])
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
    output_success(cli().args(["schema", "show"]).arg(&alias));
    let output = output_failure(
        cli()
            .args(["branch", "create", "blocked", "--store"])
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
    fs::create_dir(&outside).unwrap();
    std::os::unix::fs::symlink(&outside, &escaped).unwrap();
    for (graph, code) in [
        (&unknown, "graph_not_applied"),
        (&escaped, "cluster_graph_root_mismatch"),
    ] {
        let output = output_failure(cli().args(["schema", "show"]).arg(graph));
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
    apply_cluster_fixture(root);
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

#[test]
fn managed_invalid_acknowledgements_keep_request_identity_in_json_and_human_output() {
    for (command, mut body, field) in [
        ("apply", managed_delivery("queued"), "/data/preview_id"),
        ("apply", managed_delivery("queued"), "/meta/cluster_id"),
        ("plan", managed_preview("ready"), "/data/revision"),
    ] {
        *body.pointer_mut(field).unwrap() = serde_json::json!("foreign");
        for json in [false, true] {
            let temp = tempdir().unwrap();
            let api = IntentApiFixture::new(vec![IntentReply::json(200, body.clone())]);
            write_managed_context(temp.path(), &api.origin);
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
    let api = IntentApiFixture::new(vec![IntentReply::json(200, body)]);
    write_managed_context(temp.path(), &api.origin);
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
