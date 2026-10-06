//! Cluster lifecycle compositions over the spawned binary (recovery, drift, convergence).
//! Moved verbatim from tests/cli.rs in the modularization.

use std::fs;

use omnigraph::db::Omnigraph;
use tempfile::tempdir;

mod support;

use support::*;

/// A real listener and the real CLI own the restart-free contract. Registry
/// unit tests own exact epoch/drain/activation races; this verifies the complete
/// captured-input -> durable apply -> new schema/query/graph serving journey.
#[test]
fn cluster_e2e_live_apply_changes_schema_queries_and_adds_graph_without_restart() {
    live_apply_changes_schema_queries_and_adds_graph_without_restart(None);
}

#[test]
fn cluster_e2e_s3_live_apply_without_restart() {
    let Ok(bucket) = std::env::var("OMNIGRAPH_S3_TEST_BUCKET") else {
        eprintln!("skipping s3 live deployment: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };
    let prefix = std::env::var("OMNIGRAPH_S3_TEST_PREFIX")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "omnigraph-itests".into());
    let root = format!(
        "s3://{bucket}/{prefix}/live-deployment/{}",
        uuid::Uuid::new_v4()
    );
    live_apply_changes_schema_queries_and_adds_graph_without_restart(Some(&root));
}

#[test]
fn cluster_e2e_azure_live_apply_without_restart() {
    let Ok(container) = std::env::var("OMNIGRAPH_AZURE_TEST_CONTAINER") else {
        eprintln!("skipping azure live deployment: OMNIGRAPH_AZURE_TEST_CONTAINER is not set");
        return;
    };
    let root = format!(
        "az://{container}/omnigraph-itests/live-deployment/{}",
        uuid::Uuid::new_v4()
    );
    live_apply_changes_schema_queries_and_adds_graph_without_restart(Some(&root));
}

fn live_apply_changes_schema_queries_and_adds_graph_without_restart(storage_root: Option<&str>) {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use std::time::Duration;

    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    fs::copy(temp.path().join("people.pg"), temp.path().join("peer.pg")).unwrap();
    let config = "version: 1\nmetadata:\n  name: live-deployment\nstate:\n  backend: cluster\n  lock: true\ngraphs:\n  knowledge:\n    schema: ./people.pg\n    queries:\n      find_person:\n        file: ./people.gq\n  peer:\n    schema: ./peer.pg\n";
    let config = match storage_root {
        Some(root) => format!("storage: {root}\n{config}"),
        None => config.to_owned(),
    };
    let policies = "policies:\n  operators:\n    file: ./live.policy.yaml\n    applies_to: [cluster]\n  graph_operators:\n    file: ./graph.policy.yaml\n    applies_to: [knowledge, peer]\n";
    fs::write(temp.path().join("live.policy.yaml"), "version: 1\ngroups:\n  operators: [act-live]\nrules:\n  - id: operator\n    allow:\n      actors: { group: operators }\n      actions: [config_manage]\n").unwrap();
    fs::write(temp.path().join("graph.policy.yaml"), "version: 1\ngroups:\n  operators: [act-live]\nrules:\n  - id: operator\n    allow:\n      actors: { group: operators }\n      actions: [schema_apply, read, change, invoke_query]\n").unwrap();
    fs::write(
        temp.path().join("cluster.yaml"),
        format!("{config}{policies}"),
    )
    .unwrap();
    let root = storage_root
        .map(str::to_owned)
        .unwrap_or_else(|| format!("file://{}", temp.path().display()));
    let initial = parse_stdout_json(&output_success(
        cli()
            .args(["cluster", "apply", "--config"])
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(initial["status"], "complete", "{initial}");
    assert_eq!(initial["result"]["converged"], true, "{initial}");
    let unlock = || {
        let status = parse_stdout_json(&output_success(cli().args([
            "--cluster",
            &root,
            "cluster",
            "status",
            "--json",
        ])));
        let lock = status["lock_id"]
            .as_str()
            .expect("direct owner retains exact lock");
        output_success(cli().args([
            "--cluster",
            &root,
            "cluster",
            "force-unlock",
            lock,
            "--json",
        ]));
    };
    unlock();
    fs::write(
        temp.path().join("seed.jsonl"),
        "{\"type\":\"Person\",\"data\":{\"name\":\"Alice\",\"age\":31}}\n",
    )
    .unwrap();
    output_success(
        cli()
            .args(["--as", "act-live", "load", "--data"])
            .arg(temp.path().join("seed.jsonl"))
            .args(["--mode", "merge"])
            .arg(format!("{root}/graphs/knowledge.omni")),
    );
    unlock();

    let bindings = storage_root.is_none().then(LiveBindingsFixture::new);
    let mut server_env = vec![(
        "OMNIGRAPH_SERVER_BEARER_TOKENS_JSON",
        r#"{"act-live":"live-deployment-token","act-reader":"reader-token","act-next":"next-token"}"#,
    )];
    if let Some(bindings) = &bindings {
        server_env.extend(bindings.envs());
    }
    let server = spawn_server_with_cluster_env(temp.path(), &server_env);
    let original_pid = server.id();
    let original_url = server.base_url.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let peer_reads = Arc::new(AtomicUsize::new(0));
    let (started, ready) = std::sync::mpsc::channel();
    let peer = {
        let stop = Arc::clone(&stop);
        let reads = Arc::clone(&peer_reads);
        let base = server.base_url.clone();
        std::thread::spawn(move || {
            let client = graph_http_client();
            let mut started = Some(started);
            while !stop.load(Ordering::Acquire) {
                let response = client.post(format!("{base}/graphs/peer/query"))
                    .timeout(Duration::from_secs(5))
                    .bearer_auth("live-deployment-token")
                    .json(&serde_json::json!({"query":"query people() { match { $p: Person } return { $p.name } }", "name":"people", "params":{}}))
                    .send().unwrap();
                assert!(
                    response.status().is_success(),
                    "{}",
                    response.text().unwrap()
                );
                let result: serde_json::Value = response.json().unwrap();
                assert_eq!(result["row_count"], 0, "{result}");
                reads.fetch_add(1, Ordering::Release);
                if let Some(started) = started.take() {
                    started.send(()).unwrap();
                }
            }
        })
    };
    ready.recv_timeout(Duration::from_secs(10)).unwrap();
    let before_reads = peer_reads.load(Ordering::Acquire);
    fs::write(
        temp.path().join("people.pg"),
        "node Person { name: String @key age: I32? bio: String? }\n",
    )
    .unwrap();
    fs::write(temp.path().join("people.gq"), "query find_person($name: String) { match { $p: Person { name: $name } } return { $p.name, $p.bio } }\n").unwrap();
    fs::write(
        temp.path().join("tools.pg"),
        "node Tool { name: String @key }\n",
    )
    .unwrap();
    fs::write(
        temp.path().join("tools.gq"),
        "query tools() { match { $t: Tool } return { $t.name } }\n",
    )
    .unwrap();
    let policies = policies.replace("peer]", "peer, tools]");
    fs::write(temp.path().join("cluster.yaml"), format!("{config}  tools:\n    schema: ./tools.pg\n    queries:\n      tools:\n        file: ./tools.gq\n{policies}")).unwrap();
    let output = cli()
        .env("OMNIGRAPH_BEARER_TOKEN", "live-deployment-token")
        .args(["cluster", "apply", "--server", &server.base_url, "--config"])
        .arg(temp.path())
        .arg("--json")
        .timeout(Duration::from_secs(30))
        .output()
        .unwrap();
    stop.store(true, Ordering::Release);
    peer.join().unwrap();
    assert!(
        output.status.success(),
        "{}\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
        server.stderr()
    );
    let applied = parse_stdout_json(&output);
    assert_eq!(applied["active"], true, "{applied}");
    assert_eq!(
        applied["deployment"]["result"]["converged"], true,
        "{applied}"
    );
    assert_eq!(
        applied["deployment"]["result"]["graphs"]["tools"]["outcome"], "created",
        "{applied}"
    );
    assert!(peer_reads.load(Ordering::Acquire) > before_reads);
    assert_eq!(server.id(), original_pid);
    assert_eq!(server.base_url, original_url);
    let client = graph_http_client();
    let result: serde_json::Value = client
        .post(format!(
            "{}/graphs/knowledge/queries/find_person",
            server.base_url
        ))
        .bearer_auth("live-deployment-token")
        .json(&serde_json::json!({"params":{"name":"Alice"}}))
        .send()
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(result["row_count"], 1, "{result}");
    assert_eq!(result["rows"][0]["p.name"], "Alice");
    assert!(
        result["columns"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("p.bio")),
        "{result}"
    );
    let result: serde_json::Value = client
        .post(format!("{}/graphs/tools/queries/tools", server.base_url))
        .bearer_auth("live-deployment-token")
        .json(&serde_json::json!({"params":{}}))
        .send()
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(result["row_count"], 0, "{result}");
    let id = applied["deployment"]["result"]["id"].as_str().unwrap();
    assert!(String::from_utf8_lossy(&output.stderr).contains(&format!("Deployment-ID: {id}")));
    let status = parse_stdout_json(&output_success(
        cli()
            .env("OMNIGRAPH_BEARER_TOKEN", "live-deployment-token")
            .args([
                "cluster",
                "status",
                "--server",
                &server.base_url,
                "--deployment-id",
                id,
                "--json",
            ]),
    ));
    assert_eq!(status["active"], true, "{status}");
    assert_eq!(status["status"]["lookup"], applied["deployment"]);
    // Repeating the exact identity is observation, never a second schema or
    // graph creation invocation. Changed bytes under that identity refuse.
    let repeat = parse_stdout_json(&output_success(
        cli()
            .env("OMNIGRAPH_BEARER_TOKEN", "live-deployment-token")
            .args([
                "cluster",
                "apply",
                "--server",
                &server.base_url,
                "--deployment-id",
                id,
                "--config",
            ])
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(repeat, applied);
    let tools = temp.path().join("tools.gq");
    fs::write(&tools, format!(" {}", fs::read_to_string(&tools).unwrap())).unwrap();
    let mismatched = output_failure(
        cli()
            .env("OMNIGRAPH_BEARER_TOKEN", "live-deployment-token")
            .args([
                "cluster",
                "apply",
                "--server",
                &server.base_url,
                "--deployment-id",
                id,
                "--config",
            ])
            .arg(temp.path())
            .arg("--json"),
    );
    assert!(
        parse_stdout_json(&mismatched)["error"]
            .as_str()
            .unwrap()
            .contains("deployment_input_mismatch"),
        "{mismatched:?}"
    );
    assert_eq!(server.id(), original_pid);
    live_policy_grants_revocations_and_management_handoff(temp.path(), &server);
    if let Some(bindings) = &bindings {
        bindings.exercise(temp.path(), &server);
    }
    let retained = live_graph_removal_deletes_owned_storage(temp.path(), &server, &root);
    assert_eq!(server.id(), original_pid);
    assert_eq!(server.base_url, original_url);

    // Reboot from the durable projection: no source apply is allowed to hide
    // an activation-only change that was never actually persisted.
    let boot_deployment = live_policy_handoff_before_restart(temp.path(), &server);
    server.stop();
    unlock();
    let server = spawn_server_with_cluster_env(temp.path(), &server_env);
    let boot_status = parse_stdout_json(&output_success(live_policy_cli("next-token").args([
        "cluster",
        "status",
        "--server",
        &server.base_url,
        "--deployment-id",
        &boot_deployment,
        "--json",
    ])));
    assert_eq!(boot_status["active"], true, "{boot_status}");
    live_policy_state_survives_restart(temp.path(), &server);
    assert_live_graph_absent(&server, "retired");
    assert_eq!(live_snapshot(&server, "tools"), retained);
    assert_live_tool_data(&server);
    assert_live_person_query(&server);
    if let Some(bindings) = &bindings {
        bindings.assert_after_restart(temp.path(), &server);
    }
    if storage_root.is_none() {
        // Simulate missing storage while its owner is stopped, then prove an
        // unrelated deployment can complete without hiding unavailable graphs.
        server.stop();
        unlock();
        fs::remove_dir_all(temp.path().join("graphs/peer.omni")).unwrap();
        // Deliberately bypass the ledger while every serving writer is stopped.
        // Text equality must not hide the changed accepted schema contract.
        let _drifted = tokio::runtime::Runtime::new().unwrap().block_on(async {
            let db = Omnigraph::open(temp.path().join("graphs/knowledge.omni").to_str().unwrap())
                .await
                .unwrap();
            db.apply_schema(
                "node Person { name: String @key age: I32? bio: String? bypassed: Bool? }\n",
            )
            .await
            .unwrap();
            serde_json::to_value(db.schema_contract_digest()).unwrap()
        });
        let recovering = spawn_server_with_cluster_env(temp.path(), &server_env);
        let recovery_pid = recovering.id();
        let read_peer = || {
            client.post(format!("{}/graphs/peer/query", recovering.base_url))
            .bearer_auth("live-deployment-token")
            .json(&serde_json::json!({"query":"query people() { match { $p: Person } return { $p.name } }", "name":"people", "params":{}}))
            .send().unwrap()
        };
        assert_eq!(read_peer().status(), 503);
        // Change only the tools query; the unavailable peer is unchanged.
        fs::write(&tools, format!(" {}", fs::read_to_string(&tools).unwrap())).unwrap();
        let unrelated = parse_stdout_json(&output_failure(
            cli()
                .env("OMNIGRAPH_BEARER_TOKEN", "live-deployment-token")
                .args([
                    "cluster",
                    "apply",
                    "--server",
                    &recovering.base_url,
                    "--config",
                ])
                .arg(temp.path())
                .arg("--json"),
        ));
        assert_eq!(unrelated["deployment"]["status"], "complete", "{unrelated}");
        assert_eq!(
            unrelated["deployment"]["result"]["converged"], true,
            "{unrelated}"
        );
        assert_eq!(unrelated["active"], false, "{unrelated}");
        assert_eq!(read_peer().status(), 503);
        // Drift and missing storage cannot be repaired by accepting new identity.
        for (file, expected) in [
            ("people.pg", "applied_schema_drift"),
            ("peer.pg", "graph_unavailable"),
        ] {
            let path = temp.path().join(file);
            let source = fs::read_to_string(&path).unwrap();
            fs::write(&path, format!("{source}\n")).unwrap();
            let before = fs::read(temp.path().join("__cluster/state.json")).unwrap();
            let refusal = parse_stdout_json(&output_failure(
                cli()
                    .env("OMNIGRAPH_BEARER_TOKEN", "live-deployment-token")
                    .args([
                        "cluster",
                        "apply",
                        "--server",
                        &recovering.base_url,
                        "--config",
                    ])
                    .arg(temp.path())
                    .arg("--json"),
            ));
            assert!(refusal.to_string().contains(expected), "{refusal}");
            assert_eq!(
                fs::read(temp.path().join("__cluster/state.json")).unwrap(),
                before
            );
            fs::write(&path, source).unwrap();
        }
        assert_eq!(read_peer().status(), 503);
        assert_eq!(recovering.id(), recovery_pid);
        assert!(!temp.path().join("graphs/peer.omni").exists());
        assert_live_tool_data(&recovering);
    }
}

#[test]
fn cluster_e2e_offline_deployment_has_root_only_receipts_and_explicit_unlock() {
    for root_first in [true, false] {
        let temp = tempdir().unwrap();
        write_cluster_config_fixture(temp.path());
        let config_path = temp.path().join("cluster.yaml");
        let config = fs::read_to_string(&config_path).unwrap();
        fs::write(&config_path, config.split("policies:").next().unwrap()).unwrap();
        apply_cluster_fixture(temp.path());
        let root = format!("file://{}", temp.path().display());
        let rooted = |args: &[&str]| {
            let mut command = cli();
            command.current_dir(temp.path());
            if root_first {
                command.args(["--cluster", &root]);
            }
            command.arg("cluster").args(args);
            if !root_first {
                command.args(["--cluster", &root]);
            }
            command
        };
        // Both global-flag positions must work without mutable source files.
        fs::rename(&config_path, temp.path().join("saved-cluster.yaml")).unwrap();
        fs::rename(
            temp.path().join("people.pg"),
            temp.path().join("saved-people.pg"),
        )
        .unwrap();
        let before = fs::read(temp.path().join("__cluster/state.json")).unwrap();
        output_failure(&mut rooted(&["upgrade-ledger", "--json"]));
        assert_eq!(
            fs::read(temp.path().join("__cluster/state.json")).unwrap(),
            before
        );
        let converted = parse_stdout_json(&output_success(&mut rooted(&[
            "upgrade-ledger",
            "--writers-stopped",
            "--json",
        ])));
        assert_eq!(converted["next_sequence"], 2);
        assert_eq!(converted["result_revision"], 1);
        fs::rename(temp.path().join("saved-cluster.yaml"), &config_path).unwrap();
        fs::write(
            temp.path().join("people.pg"),
            "node Person { name: String @key age: I32? bio: String? }\n",
        )
        .unwrap();
        fs::write(temp.path().join("people.gq"), "query find_person($name: String) { match { $p: Person { name: $name } } return { $p.name, $p.bio } }\n").unwrap();
        let output = output_success(
            cli()
                .args(["--as", "operator:deploy", "cluster", "apply", "--config"])
                .arg(temp.path())
                .arg("--json"),
        );
        let deployed = parse_stdout_json(&output);
        assert_eq!(deployed["status"], "complete", "{deployed}");
        assert_eq!(deployed["result"]["converged"], true);
        let id = deployed["result"]["id"].as_str().unwrap();
        assert!(String::from_utf8_lossy(&output.stderr).contains(id));
        assert_eq!(
            deployed["result"]["graphs"]["knowledge"]["result"]["Committed"]["commit"]["actor_id"],
            "operator:deploy"
        );
        fs::remove_file(&config_path).unwrap();
        fs::remove_file(temp.path().join("people.pg")).unwrap();
        fs::remove_file(temp.path().join("people.gq")).unwrap();
        fs::create_dir(temp.path().join(".omnigraph")).unwrap();
        fs::write(temp.path().join(".omnigraph/context"), "malformed").unwrap();
        let status = parse_stdout_json(&output_success(&mut rooted(&[
            "status",
            "--deployment-id",
            id,
            "--json",
        ])));
        assert_eq!(status["lookup"], deployed);
        assert_eq!(status["next_sequence"], 3);
        let lock_id = status["lock_id"].as_str().unwrap();
        let ledger_before = fs::read(temp.path().join("__cluster/state.json")).unwrap();
        let lock_before = fs::read(temp.path().join("__cluster/lock.json")).unwrap();
        let refused = output_failure(&mut rooted(&[
            "apply",
            "--deployment-id",
            id,
            "--schema-correction",
            "missing.json",
            "--json",
        ]));
        assert_eq!(refused.status.code(), Some(2));
        assert!(String::from_utf8_lossy(&refused.stderr).contains("unexpected argument"));
        for args in [
            vec!["cluster", "upgrade-ledger", "--writers-stopped"],
            vec!["cluster", "status", "--deployment-id", id],
            vec![
                "cluster",
                "apply",
                "--deployment-id",
                id,
                "--writers-stopped",
            ],
        ] {
            let missing_root = output_failure(cli().current_dir(temp.path()).args(args));
            assert_eq!(missing_root.status.code(), Some(2));
            assert!(String::from_utf8_lossy(&missing_root.stderr).contains("requires --cluster"));
        }
        for managed_flag in [
            vec!["--no-wait"],
            vec!["--timeout", "1"],
            vec!["--idempotency-key", "key"],
        ] {
            let output = output_failure(
                rooted(&["apply", "--deployment-id", id, "--json"]).args(managed_flag),
            );
            assert_eq!(output.status.code(), Some(2));
            assert!(String::from_utf8_lossy(&output.stdout).contains("managed_scope_conflict"));
        }
        output_failure(&mut rooted(&["force-unlock", "wrong-lock", "--json"]));
        let unknown_id = format!(
            "{}:00000000000000000000000000",
            id.rsplit_once(':').unwrap().0
        );
        assert_ne!(unknown_id, id);
        let unresolved = output_failure(&mut rooted(&[
            "apply",
            "--deployment-id",
            &unknown_id,
            "--writers-stopped",
            "--json",
        ]));
        assert_eq!(
            parse_stdout_json(&unresolved)["status"],
            "identity_mismatch"
        );
        let guidance = String::from_utf8_lossy(&unresolved.stderr);
        assert!(
            guidance.contains(lock_id) && guidance.contains("exact-ID force-unlock"),
            "{guidance}"
        );
        assert_eq!(
            fs::read(temp.path().join("__cluster/state.json")).unwrap(),
            ledger_before
        );
        assert_eq!(
            fs::read(temp.path().join("__cluster/lock.json")).unwrap(),
            lock_before
        );
        let repeated = parse_stdout_json(&output_success(&mut rooted(&[
            "apply",
            "--deployment-id",
            id,
            "--json",
        ])));
        assert_eq!(
            repeated, deployed,
            "terminal recovery is lookup only and needs no mutable files"
        );
        let unlocked = parse_stdout_json(&output_success(&mut rooted(&[
            "force-unlock",
            lock_id,
            "--json",
        ])));
        assert_eq!(unlocked["unlocked"], true);
        let status = parse_stdout_json(&output_success(&mut rooted(&["status", "--json"])));
        assert!(status["lock_id"].is_null());
        assert_eq!(status["next_sequence"], 3);
        let conflict = output_failure(&mut rooted(&["status", "--config", "."]));
        assert_eq!(conflict.status.code(), Some(2));
        assert!(
            String::from_utf8_lossy(&conflict.stderr).contains("--cluster and explicit --config")
        );
    }
}

#[test]
fn cluster_e2e_apply_schema_query_and_noop_each_have_exact_receipts() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    let first = apply_cluster_fixture(temp.path());
    assert_eq!(first["result"]["graphs"]["knowledge"]["outcome"], "created");
    fs::write(
        temp.path().join("people.pg"),
        "node Person { name: String @key age: I32? bio: String? }\n",
    )
    .unwrap();
    fs::write(temp.path().join("people.gq"),
        "query find_person($name: String) { match { $p: Person { name: $name } } return { $p.name, $p.bio } }\n").unwrap();
    let evolved = apply_cluster_fixture(temp.path());
    assert_ne!(first["result"]["id"], evolved["result"]["id"]);
    let schema = output_success(
        cli()
            .args(["schema", "show"])
            .arg(temp.path().join("graphs/knowledge.omni")),
    );
    assert!(stdout_string(&schema).contains("bio"));
    let plan = cluster_json(temp.path(), "plan");
    assert!(plan["changes"].as_array().unwrap().is_empty(), "{plan}");
    let repeated = apply_cluster_fixture(temp.path());
    assert!(repeated["result"]["graphs"].as_object().unwrap().is_empty());
    assert_ne!(repeated["result"]["id"], evolved["result"]["id"]);
}

#[test]
fn cluster_e2e_force_unlock_unblocks_apply() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    apply_cluster_fixture(temp.path());
    write_cluster_lock(temp.path(), "stuck-lock", "apply");
    let before = fs::read(temp.path().join("__cluster/state.json")).unwrap();
    let refused = parse_stdout_json(&output_failure(
        cli()
            .args(["cluster", "apply", "--config"])
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(
        refused["diagnostics"][0]["code"], "state_lock_held",
        "{refused}"
    );
    assert_eq!(
        fs::read(temp.path().join("__cluster/state.json")).unwrap(),
        before
    );
    unlock_cluster_fixture(temp.path());
    apply_cluster_fixture(temp.path());
}

#[test]
fn cluster_e2e_lost_ledger_does_not_adopt_existing_graphs() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    apply_cluster_fixture(temp.path());
    let graph = temp.path().join("graphs/knowledge.omni");
    let before = manifest_dataset_version(&graph);
    fs::remove_file(temp.path().join("__cluster/state.json")).unwrap();
    let refused = output_failure(
        cli()
            .args(["cluster", "apply", "--config"])
            .arg(temp.path())
            .arg("--json"),
    );
    assert!(
        parse_stdout_json(&refused)["diagnostics"][0]["code"] == "graph_root_exists",
        "{refused:?}"
    );
    assert_eq!(manifest_dataset_version(&graph), before);
    assert!(!temp.path().join("__cluster/lock.json").exists());
}

#[test]
fn cluster_e2e_destroyed_graph_is_not_silently_recreated() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    apply_cluster_fixture(temp.path());
    let graph = temp.path().join("graphs/knowledge.omni");
    let before = fs::read(temp.path().join("__cluster/state.json")).unwrap();
    fs::remove_dir_all(&graph).unwrap();
    // A query change makes this graph affected. An unrelated/no-op deployment
    // must not open every declared graph merely to check its availability.
    fs::write(
        temp.path().join("people.gq"),
        "query find_person($name: String) {
  match { $p: Person { name: $name } }
  return { $p.name }
}
",
    )
    .unwrap();
    let refused = output_failure(
        cli()
            .args(["cluster", "apply", "--config"])
            .arg(temp.path())
            .arg("--json"),
    );
    assert_eq!(
        parse_stdout_json(&refused)["diagnostics"][0]["code"],
        "graph_unavailable"
    );
    assert_eq!(
        fs::read(temp.path().join("__cluster/state.json")).unwrap(),
        before
    );
    assert!(!graph.exists());
    assert!(!temp.path().join("__cluster/lock.json").exists());
}

#[test]
fn cluster_e2e_declared_graph_removal_deletes_owned_storage() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    apply_cluster_fixture(temp.path());
    let config = temp.path().join("cluster.yaml");
    let source = fs::read_to_string(&config).unwrap();
    fs::write(
        temp.path().join("service.pg"),
        "node Service { name: String @key }\n",
    )
    .unwrap();
    let expanded = source.replace(
        "policies:",
        "  engineering:\n    schema: ./service.pg\npolicies:",
    );
    fs::write(&config, &expanded).unwrap();
    let created = apply_cluster_fixture(temp.path());
    assert_eq!(
        created["result"]["graphs"]["engineering"]["outcome"],
        "created"
    );
    assert!(
        temp.path()
            .join("graphs/engineering.omni/__manifest")
            .exists()
    );
    let graph = temp.path().join("graphs/engineering.omni");
    output_success(
        cli()
            .args([
                "mutate",
                "seed",
                "-e",
                "query seed() { insert Service { name: \"Deleted\" } }",
                "--store",
            ])
            .arg(&graph),
    );
    unlock_cluster_fixture(temp.path());
    output_success(
        cli()
            .args(["branch", "create", "retained-history", "--uri"])
            .arg(&graph),
    );
    unlock_cluster_fixture(temp.path());
    let peer_before = tokio::runtime::Runtime::new().unwrap().block_on(async {
        let db =
            Omnigraph::open_read_only(temp.path().join("graphs/knowledge.omni").to_str().unwrap())
                .await
                .unwrap();
        db.schema_contract_digest()
    });
    let external = temp.path().join("external-blob");
    fs::write(&external, "external owner").unwrap();
    fs::write(&config, &source).unwrap();
    let plan = parse_stdout_json(&output_success(
        cli()
            .args(["cluster", "plan", "--config"])
            .arg(temp.path())
            .arg("--json"),
    ));
    assert!(
        plan["changes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|change| change["resource"] == "graph.engineering"
                && change["operation"] == "delete"),
        "{plan}"
    );
    assert!(graph.exists(), "planning must not delete storage");
    let deleted = apply_cluster_fixture(temp.path());
    assert_eq!(
        deleted["result"]["graphs"]["engineering"]["outcome"], "deleted",
        "{deleted}"
    );
    assert!(!graph.exists());
    assert!(
        temp.path()
            .join("graphs/knowledge.omni/__manifest")
            .exists()
    );
    let peer_after = tokio::runtime::Runtime::new().unwrap().block_on(async {
        let db =
            Omnigraph::open_read_only(temp.path().join("graphs/knowledge.omni").to_str().unwrap())
                .await
                .unwrap();
        db.schema_contract_digest()
    });
    assert_eq!(peer_after, peer_before);
    assert_eq!(fs::read_to_string(external).unwrap(), "external owner");
    let repeat = apply_cluster_fixture(temp.path());
    assert!(
        repeat["result"]["graphs"].as_object().unwrap().is_empty(),
        "{repeat}"
    );
    assert!(!graph.exists());
    assert!(!temp.path().join("__cluster/lock.json").exists());
}

#[test]
fn cluster_e2e_removed_v1_commands_are_not_executable() {
    for command in ["import", "refresh", "approve"] {
        let temp = tempdir().unwrap();
        write_cluster_config_fixture(temp.path());
        let refused = output_failure(
            cli()
                .args(["cluster", command, "--config"])
                .arg(temp.path()),
        );
        assert_eq!(refused.status.code(), Some(2));
        assert!(String::from_utf8_lossy(&refused.stderr).contains("unrecognized subcommand"));
        assert!(!temp.path().join("__cluster").exists());
    }
}

#[test]
fn cluster_e2e_payload_drift_requires_authoritative_restore() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    apply_cluster_fixture(temp.path());
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(temp.path().join("__cluster/state.json")).unwrap())
            .unwrap();
    let digest = state["applied_revision"]["resources"]["query.knowledge.find_person"]["digest"]
        .as_str()
        .unwrap();
    let blob = temp
        .path()
        .join("__cluster/resources/query/knowledge/find_person")
        .join(format!("{digest}.gq"));
    let expected = fs::read(&blob).unwrap();
    fs::remove_file(&blob).unwrap();
    let status = cluster_json(temp.path(), "status");
    assert!(
        status["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|diagnostic| diagnostic["code"] == "catalog_payload_missing"),
        "{status}"
    );
    apply_cluster_fixture(temp.path());
    assert!(
        !blob.exists(),
        "unchanged configuration must not repair untargeted payloads"
    );
    // Restore the authoritative immutable payload from backup; apply has no
    // repair override that could substitute a different policy or query.
    fs::write(&blob, &expected).unwrap();
    let clean = cluster_json(temp.path(), "status");
    assert!(
        clean["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .all(|diagnostic| !diagnostic["code"]
                .as_str()
                .unwrap()
                .starts_with("catalog_payload")),
        "{clean}"
    );
}

fn live_apply(config_dir: &std::path::Path, server: &TestServer, token: &str) -> serde_json::Value {
    let mut command = cli();
    command
        .env("OMNIGRAPH_BEARER_TOKEN", token)
        .args(["cluster", "apply", "--server", &server.base_url, "--config"])
        .arg(config_dir)
        .arg("--json")
        .timeout(std::time::Duration::from_secs(30));
    let result = parse_stdout_json(&output_success(&mut command));
    assert_eq!(result["active"], true, "{result}");
    assert_eq!(
        result["deployment"]["result"]["converged"], true,
        "{result}"
    );
    result
}

fn live_get(server: &TestServer, path: &str) -> serde_json::Value {
    let response = graph_http_client()
        .get(format!("{}{path}", server.base_url))
        .bearer_auth("live-deployment-token")
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .unwrap();
    let status = response.status();
    let body = response.text().unwrap();
    assert!(status.is_success(), "{path}: {status}: {body}");
    serde_json::from_str(&body).unwrap()
}

fn live_snapshot(server: &TestServer, graph: &str) -> serde_json::Value {
    live_get(server, &format!("/graphs/{graph}/snapshot"))
}

fn assert_live_tool_data(server: &TestServer) {
    let result = parse_stdout_json(&output_success(
        live_policy_cli("live-deployment-token").args([
            "query",
            "tools",
            "--server",
            &server.base_url,
            "--graph",
            "tools",
            "--json",
        ]),
    ));
    assert_eq!(result["row_count"], 1, "{result}");
    assert_eq!(result["rows"][0]["t.name"], "Retained", "{result}");
}

fn assert_live_person_query(server: &TestServer) {
    let result = parse_stdout_json(&output_success(
        live_policy_cli("live-deployment-token").args([
            "query",
            "find_person",
            "--server",
            &server.base_url,
            "--graph",
            "knowledge",
            "--params",
            r#"{"name":"Alice"}"#,
            "--json",
        ]),
    ));
    assert_eq!(result["row_count"], 1, "{result}");
    assert_eq!(result["rows"][0]["p.name"], "Alice", "{result}");
}

fn assert_live_graph_absent(server: &TestServer, graph: &str) {
    let response = graph_http_client()
        .get(format!("{}/graphs/{graph}/snapshot", server.base_url))
        .bearer_auth("live-deployment-token")
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .unwrap();
    assert_eq!(response.status(), 404, "{}", response.text().unwrap());
}

fn live_graph_removal_deletes_owned_storage(
    config_dir: &std::path::Path,
    server: &TestServer,
    root: &str,
) -> serde_json::Value {
    output_success(live_policy_cli("live-deployment-token").args([
        "mutate",
        "seed",
        "--server",
        &server.base_url,
        "--graph",
        "tools",
        "-e",
        "query seed() { insert Tool { name: \"Retained\" } }",
        "--json",
    ]));
    let snapshot = live_snapshot(server, "tools");
    let history = live_get(server, "/graphs/tools/commits");
    assert_live_tool_data(server);
    let config_path = config_dir.join("cluster.yaml");
    let source = fs::read_to_string(&config_path).unwrap();
    let mut expanded: serde_yaml::Value = serde_yaml::from_str(&source).unwrap();
    expanded["graphs"]["retired"] = expanded["graphs"]["tools"].clone();
    expanded["policies"]["graph_operators"]["applies_to"]
        .as_sequence_mut()
        .unwrap()
        .push(serde_yaml::Value::from("retired"));
    fs::write(&config_path, serde_yaml::to_string(&expanded).unwrap()).unwrap();
    live_apply(config_dir, server, "live-deployment-token");
    output_success(live_policy_cli("live-deployment-token").args([
        "mutate",
        "seed",
        "--server",
        &server.base_url,
        "--graph",
        "retired",
        "-e",
        "query seed() { insert Tool { name: \"Deleted\" } }",
        "--json",
    ]));
    assert!(live_get(server, "/graphs/retired/commits")["commits"].is_array());
    fs::write(&config_path, &source).unwrap();

    // Candidate permissions cannot authorize deletion. The current policy
    // denies this caller and both serving state and durable files stay intact.
    let status_before = live_policy_status(server, "live-deployment-token");
    let deleted_before = live_snapshot(server, "retired");
    live_policy_expect_forbidden(
        live_policy_cli("reader-token")
            .args(["cluster", "apply", "--server", &server.base_url, "--config"])
            .arg(config_dir)
            .arg("--json"),
    );
    assert_eq!(
        live_policy_status(server, "live-deployment-token"),
        status_before
    );
    assert_eq!(live_snapshot(server, "retired"), deleted_before);

    let deleted = live_apply(config_dir, server, "live-deployment-token");
    assert_eq!(
        deleted["deployment"]["result"]["graphs"]["retired"]["outcome"], "deleted",
        "{deleted}"
    );
    assert_live_graph_absent(server, "retired");
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let storage = omnigraph::storage::storage_for_uri(root).unwrap();
        assert!(
            !storage
                .exists(&format!("{root}/graphs/retired.omni"))
                .await
                .unwrap()
        );
    });
    assert_eq!(live_snapshot(server, "tools"), snapshot);
    assert_eq!(live_get(server, "/graphs/tools/commits"), history);
    assert_live_tool_data(server);

    // Reusing the original ID only returns the deletion receipt.
    let id = deleted["deployment"]["result"]["id"].as_str().unwrap();
    let repeat = parse_stdout_json(&output_success(
        live_policy_cli("live-deployment-token")
            .args([
                "cluster",
                "apply",
                "--server",
                &server.base_url,
                "--deployment-id",
                id,
                "--config",
            ])
            .arg(config_dir)
            .arg("--json"),
    ));
    assert_eq!(repeat, deleted);

    // A later declaration creates a new, empty graph lifetime. An observation
    // of the old deletion ID must never delete this replacement.
    fs::write(&config_path, serde_yaml::to_string(&expanded).unwrap()).unwrap();
    let created = live_apply(config_dir, server, "live-deployment-token");
    assert_eq!(
        created["deployment"]["result"]["graphs"]["retired"]["outcome"],
        "created"
    );
    let replacement = live_snapshot(server, "retired");
    assert_ne!(replacement, deleted_before);
    let empty = parse_stdout_json(&output_success(
        live_policy_cli("live-deployment-token").args([
            "query",
            "tools",
            "--server",
            &server.base_url,
            "--graph",
            "retired",
            "--json",
        ]),
    ));
    assert_eq!(empty["row_count"], 0, "{empty}");
    fs::write(&config_path, &source).unwrap();
    let observed = parse_stdout_json(&output_failure(
        live_policy_cli("live-deployment-token")
            .args([
                "cluster",
                "apply",
                "--server",
                &server.base_url,
                "--deployment-id",
                id,
                "--config",
            ])
            .arg(config_dir)
            .arg("--json"),
    ));
    assert_eq!(observed["active"], false, "{observed}");
    assert_eq!(observed["deployment"], deleted["deployment"]);
    assert_eq!(live_snapshot(server, "retired"), replacement);
    live_apply(config_dir, server, "live-deployment-token");
    assert_live_graph_absent(server, "retired");
    snapshot
}

/// Exercise policy cutover through the public CLI and a real TCP listener.
/// The existing journey owns process startup and the later restart.
fn live_policy_grants_revocations_and_management_handoff(
    config_dir: &std::path::Path,
    server: &TestServer,
) {
    let original_pid = server.id();
    let original_url = server.base_url.clone();
    let graph_policy_path = config_dir.join("graph.policy.yaml");
    let management_path = config_dir.join("live.policy.yaml");
    let original_graph_policy = fs::read_to_string(&graph_policy_path).unwrap();
    let original_management = fs::read_to_string(&management_path).unwrap();

    let before = live_snapshot(server, "knowledge");
    live_policy_expect_forbidden(&mut live_policy_mutation(server, "PolicyBeforeGrant"));
    assert_eq!(live_snapshot(server, "knowledge"), before);
    let before_status = live_policy_status(server, "live-deployment-token");

    let granted = format!(
        "{}  - id: lifecycle-reader\n    allow:\n      actors: {{ group: lifecycle-readers }}\n      actions: [read, change]\n",
        original_graph_policy.replace("groups:\n", "groups:\n  lifecycle-readers: [act-reader]\n")
    );
    fs::write(&graph_policy_path, &granted).unwrap();
    fs::write(
        &management_path,
        original_management.replace("[act-live]", "[act-reader]"),
    )
    .unwrap();
    live_policy_expect_forbidden(
        live_policy_cli("reader-token")
            .args(["cluster", "apply", "--server", &server.base_url, "--config"])
            .arg(config_dir)
            .arg("--json"),
    );
    assert_eq!(
        live_policy_status(server, "live-deployment-token"),
        before_status
    );
    assert_eq!(live_snapshot(server, "knowledge"), before);

    // The current administrator can grant data access; the candidate could
    // not authorize its own submission or publish the attempted mutation.
    fs::write(&management_path, &original_management).unwrap();
    live_apply(config_dir, server, "live-deployment-token");
    let mutation = parse_stdout_json(&output_success(&mut live_policy_mutation(
        server,
        "PolicyGranted",
    )));
    assert!(
        mutation["commit"]["graph_commit_id"].is_string(),
        "{mutation}"
    );
    live_policy_assert_granted_row(server);

    fs::write(
        &graph_policy_path,
        granted.replace("actions: [read, change]", "actions: [read]"),
    )
    .unwrap();
    live_apply(config_dir, server, "live-deployment-token");
    let before = live_snapshot(server, "knowledge");
    live_policy_expect_forbidden(&mut live_policy_mutation(server, "PolicyAfterRevocation"));
    assert_eq!(live_snapshot(server, "knowledge"), before);
    live_policy_assert_granted_row(server);

    // Management authority moves independently of graph permissions. act-next
    // has no graph grant and must still be able to observe and restore policy.
    fs::write(
        &management_path,
        original_management.replace("[act-live]", "[act-next]"),
    )
    .unwrap();
    let handoff = live_apply(config_dir, server, "live-deployment-token");
    let current = live_policy_status(server, "next-token");
    assert_eq!(
        current["status"]["result_revision"],
        handoff["deployment"]["result"]["result_revision"]
    );
    live_policy_expect_forbidden(live_policy_cli("live-deployment-token").args([
        "cluster",
        "status",
        "--server",
        &server.base_url,
        "--json",
    ]));
    fs::write(&management_path, &original_management).unwrap();
    live_policy_expect_forbidden(
        live_policy_cli("live-deployment-token")
            .args(["cluster", "apply", "--server", &server.base_url, "--config"])
            .arg(config_dir)
            .arg("--json"),
    );
    assert_eq!(live_policy_status(server, "next-token"), current);
    live_apply(config_dir, server, "next-token");
    live_policy_status(server, "live-deployment-token");
    live_policy_expect_forbidden(live_policy_cli("next-token").args([
        "cluster",
        "status",
        "--server",
        &server.base_url,
        "--json",
    ]));
    assert_eq!(server.id(), original_pid);
    assert_eq!(server.base_url, original_url);
}

fn live_policy_handoff_before_restart(config_dir: &std::path::Path, server: &TestServer) -> String {
    let management_path = config_dir.join("live.policy.yaml");
    let current = fs::read_to_string(&management_path).unwrap();
    let handoff = current.replace("[act-live]", "[act-next]");
    assert_ne!(
        handoff, current,
        "fixture must still grant act-live management"
    );
    fs::write(management_path, handoff).unwrap();
    let result = live_apply(config_dir, server, "live-deployment-token");
    live_policy_expect_forbidden(live_policy_cli("live-deployment-token").args([
        "cluster",
        "status",
        "--server",
        &server.base_url,
        "--json",
    ]));
    live_policy_status(server, "next-token");
    result["deployment"]["result"]["id"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn live_policy_state_survives_restart(config_dir: &std::path::Path, server: &TestServer) {
    let before = live_snapshot(server, "knowledge");
    live_policy_expect_forbidden(&mut live_policy_mutation(server, "PolicyAfterRestart"));
    assert_eq!(live_snapshot(server, "knowledge"), before);
    live_policy_assert_granted_row(server);
    live_policy_expect_forbidden(live_policy_cli("live-deployment-token").args([
        "cluster",
        "status",
        "--server",
        &server.base_url,
        "--json",
    ]));
    live_policy_status(server, "next-token");
    let management_path = config_dir.join("live.policy.yaml");
    let handoff = fs::read_to_string(&management_path).unwrap();
    let restored = handoff.replace("[act-next]", "[act-live]");
    assert_ne!(restored, handoff, "restart fixture must retain the handoff");
    fs::write(management_path, restored).unwrap();
    live_apply(config_dir, server, "next-token");
    live_policy_status(server, "live-deployment-token");
    live_policy_expect_forbidden(live_policy_cli("next-token").args([
        "cluster",
        "status",
        "--server",
        &server.base_url,
        "--json",
    ]));
}

fn live_policy_cli(token: &str) -> assert_cmd::Command {
    let mut command = cli();
    command
        .env("OMNIGRAPH_BEARER_TOKEN", token)
        .timeout(std::time::Duration::from_secs(30));
    command
}

fn live_policy_mutation(server: &TestServer, name: &str) -> assert_cmd::Command {
    let mut command = live_policy_cli("reader-token");
    command.args([
        "mutate",
        "policy_probe",
        "--server",
        &server.base_url,
        "--graph",
        "knowledge",
        "-e",
        "query policy_probe($name: String) { insert Person { name: $name } }",
        "--params",
        &serde_json::json!({"name":name}).to_string(),
        "--json",
    ]);
    command
}

fn live_policy_expect_forbidden(command: &mut assert_cmd::Command) {
    let output = output_failure(command);
    let error = parse_stdout_json(&output);
    let message = error["error"]
        .as_str()
        .expect("CLI error message")
        .to_ascii_lowercase();
    assert!(
        message.contains("forbidden") || message.contains("policy denied"),
        "expected an authorization refusal, got {error}"
    );
}

fn live_policy_status(server: &TestServer, token: &str) -> serde_json::Value {
    parse_stdout_json(&output_success(live_policy_cli(token).args([
        "cluster",
        "status",
        "--server",
        &server.base_url,
        "--json",
    ])))
}

fn live_policy_assert_granted_row(server: &TestServer) {
    let rows = parse_stdout_json(&output_success(live_policy_cli("reader-token").args([
        "query",
        "policy_read",
        "--server",
        &server.base_url,
        "--graph",
        "knowledge",
        "-e",
        "query policy_read($name: String) { match { $p: Person { name: $name } } return { $p.name } }",
        "--params",
        r#"{"name":"PolicyGranted"}"#,
        "--json",
    ])));
    assert_eq!(rows["row_count"], 1, "{rows}");
    assert_eq!(rows["rows"][0]["p.name"], "PolicyGranted", "{rows}");
}

/// Bounded loopback dependencies for the existing local live-deployment journey.
/// The S3 fixture serves HEAD only: overwrite retains the descriptor, while the
/// engine still probes its size before publishing. No cloud credentials or
/// external object service participate in this test.
struct LiveBindingsFixture {
    first: support::managed_http::IntentApiFixture,
    replacement: support::managed_http::IntentApiFixture,
    blobs: support::managed_http::IntentApiFixture,
    final_snapshot: std::cell::RefCell<Option<serde_json::Value>>,
}

impl LiveBindingsFixture {
    fn new() -> Self {
        use support::managed_http::{IntentApiFixture, IntentReply};
        let embedding = |vector| {
            IntentReply::json(
                200,
                serde_json::json!({"data":[{"index":0,"embedding":vector}]}),
            )
        };
        let head = IntentReply {
            status: 200,
            headers: vec![
                ("content-length".into(), "8".into()),
                (
                    "last-modified".into(),
                    "Mon, 05 Oct 2026 00:00:00 GMT".into(),
                ),
                ("etag".into(), "\"lifecycle-external\"".into()),
            ],
            body: Vec::new(),
        };
        Self {
            first: IntentApiFixture::new(vec![
                embedding([1.0, 0.0, 0.0]),
                embedding([1.0, 0.0, 0.0]),
                embedding([0.0, 1.0, 0.0]),
            ]),
            // Rebinding, refused replacement, and post-restart checks each
            // execute a real query. Every call is counted below.
            replacement: IntentApiFixture::new(vec![embedding([1.0, 0.0, 0.0]); 4]),
            blobs: IntentApiFixture::new(vec![head.clone(), head]),
            final_snapshot: std::cell::RefCell::new(None),
        }
    }

    fn envs(&self) -> Vec<(&str, &str)> {
        vec![
            ("OMNIGRAPH_LIFECYCLE_EMBED_KEY", "test-lifecycle-key"),
            ("OMNIGRAPH_EMBEDDINGS_MOCK", "0"),
            ("OMNIGRAPH_EMBED_RETRY_ATTEMPTS", "1"),
            ("OMNIGRAPH_EMBED_TIMEOUT_MS", "2000"),
            ("OMNIGRAPH_EMBED_DEADLINE_MS", "3000"),
            ("AWS_ACCESS_KEY_ID", "lifecycle-test-key"),
            ("AWS_SECRET_ACCESS_KEY", "lifecycle-test-secret"),
            ("AWS_SESSION_TOKEN", ""),
            (
                "AWS_CONFIG_FILE",
                "/nonexistent/omnigraph-lifecycle-aws-config",
            ),
            (
                "AWS_SHARED_CREDENTIALS_FILE",
                "/nonexistent/omnigraph-lifecycle-aws-credentials",
            ),
            ("AWS_REGION", "us-east-1"),
            ("AWS_DEFAULT_REGION", "us-east-1"),
            ("AWS_ENDPOINT", self.blobs.origin.as_str()),
            ("AWS_ENDPOINT_URL", self.blobs.origin.as_str()),
            ("AWS_ENDPOINT_URL_S3", self.blobs.origin.as_str()),
            ("AWS_ALLOW_HTTP", "true"),
            ("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "false"),
            ("AWS_S3_FORCE_PATH_STYLE", "true"),
            ("AWS_EC2_METADATA_DISABLED", "true"),
            ("OBJECT_STORE_CLIENT_MAX_RETRIES", "0"),
            ("OBJECT_STORE_CLIENT_RETRY_TIMEOUT", "2"),
        ]
    }

    fn exercise(&self, config_dir: &std::path::Path, server: &TestServer) {
        let token = "live-deployment-token";
        let first_profile = serde_yaml::from_str::<serde_yaml::Value>(&format!(
            "kind: openai-compatible\nbase_url: {}/v1\nmodel: space-a\napi_key: ${{OMNIGRAPH_LIFECYCLE_EMBED_KEY}}\n",
            self.first.origin
        )).unwrap();
        fs::write(config_dir.join("bindings.pg"), "node SearchDoc { name: String @key text: String embedding: Vector(3) @embed(\"text\") }\nnode Asset { name: String @key content: Blob? }\n").unwrap();
        bindings_edit_config(config_dir, |config| {
            config["graphs"]["bindings"] = serde_yaml::from_str("schema: ./bindings.pg\nembedding_provider: first\nexternal_blobs:\n  allow:\n    - base: s3://lifecycle-assets/first\n      scope: server_safe\n").unwrap();
            config["providers"] = serde_yaml::from_str("embedding: {}\n").unwrap();
            config["providers"]["embedding"]["first"] = first_profile;
            config["policies"]["graph_operators"]["applies_to"]
                .as_sequence_mut()
                .unwrap()
                .push("bindings".into());
        });
        live_apply(config_dir, server, token);
        let data = config_dir.join("binding-vectors.jsonl");
        fs::write(&data, "{\"type\":\"SearchDoc\",\"data\":{\"name\":\"alpha\",\"text\":\"first\",\"embedding\":[1,0,0]}}\n{\"type\":\"SearchDoc\",\"data\":{\"name\":\"beta\",\"text\":\"second\",\"embedding\":[0,1,0]}}\n").unwrap();
        output_success(
            cli()
                .env("OMNIGRAPH_BEARER_TOKEN", token)
                .args([
                    "load",
                    "--server",
                    &server.base_url,
                    "--graph",
                    "bindings",
                    "--mode",
                    "overwrite",
                    "--yes",
                    "--data",
                ])
                .arg(data)
                .timeout(std::time::Duration::from_secs(30)),
        );
        let before = live_snapshot(server, "bindings");
        bindings_nearest(server, "alpha");
        bindings_nearest(server, "alpha");
        self.assert_embedding_requests(&self.first, &["space-a", "space-a"], "/v1/embeddings");

        // Replacing an already-used profile must replace its lazy engine client.
        bindings_edit_config(config_dir, |config| {
            config["providers"]["embedding"]["first"]["model"] = "space-b".into();
        });
        live_apply(config_dir, server, token);
        bindings_nearest(server, "beta");
        self.assert_embedding_requests(
            &self.first,
            &["space-a", "space-a", "space-b"],
            "/v1/embeddings",
        );
        assert_eq!(live_snapshot(server, "bindings"), before);

        // A new endpoint and graph binding also remove the former definition.
        let replacement = serde_yaml::from_str::<serde_yaml::Value>(&format!(
            "kind: openai-compatible\nbase_url: {}/v2\nmodel: space-c\napi_key: ${{OMNIGRAPH_LIFECYCLE_EMBED_KEY}}\n",
            self.replacement.origin
        )).unwrap();
        bindings_edit_config(config_dir, |config| {
            config["providers"]["embedding"]
                .as_mapping_mut()
                .unwrap()
                .remove(serde_yaml::Value::from("first"));
            config["providers"]["embedding"]["replacement"] = replacement;
            config["graphs"]["bindings"]["embedding_provider"] = "replacement".into();
        });
        live_apply(config_dir, server, token);
        bindings_nearest(server, "alpha");
        self.assert_embedding_requests(&self.replacement, &["space-c"], "/v2/embeddings");
        self.first.assert_complete();
        assert_eq!(live_snapshot(server, "bindings"), before);

        // Missing server credentials refuse before acceptance and keep the
        // already-used replacement provider working under its existing binding.
        let config_path = config_dir.join("cluster.yaml");
        let valid_config = fs::read(&config_path).unwrap();
        let ledger = fs::read(config_dir.join("__cluster/state.json")).unwrap();
        let missing = format!(
            "OMNIGRAPH_MISSING_LIFECYCLE_{}",
            uuid::Uuid::new_v4().simple()
        );
        assert!(std::env::var_os(&missing).is_none());
        bindings_edit_config(config_dir, |config| {
            config["providers"]["embedding"]["replacement"]["api_key"] =
                format!("${{{missing}}}").into();
        });
        let refused = output_failure(
            cli()
                .env("OMNIGRAPH_BEARER_TOKEN", token)
                .args(["cluster", "apply", "--server", &server.base_url, "--config"])
                .arg(config_dir)
                .arg("--json")
                .timeout(std::time::Duration::from_secs(30)),
        );
        assert!(stdout_string(&refused).contains(&missing), "{refused:?}");
        assert_eq!(
            fs::read(config_dir.join("__cluster/state.json")).unwrap(),
            ledger
        );
        assert_eq!(live_snapshot(server, "bindings"), before);
        assert_eq!(self.replacement.requests().len(), 1);
        fs::write(config_path, valid_config).unwrap();
        bindings_nearest(server, "alpha");
        self.assert_embedding_requests(
            &self.replacement,
            &["space-c", "space-c"],
            "/v2/embeddings",
        );

        bindings_external_load(server, "s3://lifecycle-assets/first/asset.bin", true);
        self.assert_blob_probes(&["/lifecycle-assets/first/asset.bin"]);
        bindings_external_redirect(server, "s3://lifecycle-assets/first/asset.bin");
        let first_blob = live_snapshot(server, "bindings");
        bindings_edit_config(config_dir, |config| {
            config["graphs"]["bindings"]["external_blobs"]["allow"][0]["base"] =
                "s3://lifecycle-assets/second".into();
        });
        live_apply(config_dir, server, token);
        assert_eq!(live_snapshot(server, "bindings"), first_blob);
        bindings_external_load(server, "s3://lifecycle-assets/first/asset.bin", false);
        assert_eq!(live_snapshot(server, "bindings"), first_blob);
        self.assert_blob_probes(&["/lifecycle-assets/first/asset.bin"]);
        bindings_external_redirect(server, "s3://lifecycle-assets/first/asset.bin");

        bindings_external_load(server, "s3://lifecycle-assets/second/asset.bin", true);
        self.assert_blob_probes(&[
            "/lifecycle-assets/first/asset.bin",
            "/lifecycle-assets/second/asset.bin",
        ]);
        let final_snapshot = live_snapshot(server, "bindings");
        // A server must not promote an embedded-only source to server-safe.
        bindings_edit_config(config_dir, |config| {
            config["graphs"]["bindings"]["external_blobs"]["allow"][0]["scope"] =
                "embedded_only".into();
        });
        live_apply(config_dir, server, token);
        bindings_external_load(server, "s3://lifecycle-assets/second/asset.bin", false);
        assert_eq!(live_snapshot(server, "bindings"), final_snapshot);
        bindings_edit_config(config_dir, |config| {
            config["graphs"]["bindings"]
                .as_mapping_mut()
                .unwrap()
                .remove(serde_yaml::Value::from("external_blobs"));
        });
        live_apply(config_dir, server, token);
        bindings_external_load(server, "s3://lifecycle-assets/second/asset.bin", false);
        bindings_external_redirect(server, "s3://lifecycle-assets/second/asset.bin");
        assert_eq!(live_snapshot(server, "bindings"), final_snapshot);
        self.blobs.assert_complete();
        *self.final_snapshot.borrow_mut() = Some(final_snapshot);
    }

    fn assert_after_restart(&self, _config_dir: &std::path::Path, server: &TestServer) {
        let previous = self.replacement.requests().len();
        bindings_nearest(server, "alpha");
        assert_eq!(self.replacement.requests().len(), previous + 1);
        self.assert_embedding_requests(
            &self.replacement,
            &vec!["space-c"; previous + 1],
            "/v2/embeddings",
        );
        self.first.assert_complete();
        bindings_external_load(server, "s3://lifecycle-assets/second/asset.bin", false);
        bindings_external_redirect(server, "s3://lifecycle-assets/second/asset.bin");
        self.blobs.assert_complete();
        assert_eq!(
            live_snapshot(server, "bindings"),
            *self.final_snapshot.borrow().as_ref().unwrap()
        );
    }

    fn assert_embedding_requests(
        &self,
        fixture: &support::managed_http::IntentApiFixture,
        models: &[&str],
        path: &str,
    ) {
        let requests = fixture.requests();
        assert_eq!(requests.len(), models.len());
        for (request, model) in requests.iter().zip(models) {
            assert_eq!(request.method, "POST");
            assert_eq!(request.path, path);
            assert_eq!(
                request.headers["authorization"],
                "Bearer test-lifecycle-key"
            );
            assert_eq!(request.body["model"], *model);
            assert_eq!(
                request.body["input"],
                serde_json::json!(["same-query-text"])
            );
            assert_eq!(request.body["dimensions"], 3);
        }
    }

    fn assert_blob_probes(&self, paths: &[&str]) {
        let requests = self.blobs.requests();
        assert_eq!(
            requests.len(),
            paths.len(),
            "external I/O must match admitted sources"
        );
        for (request, path) in requests.iter().zip(paths) {
            assert_eq!(
                request.method, "HEAD",
                "overwrite must retain the descriptor without fetching its body"
            );
            assert_eq!(request.path, *path);
        }
    }
}

fn bindings_edit_config(config_dir: &std::path::Path, edit: impl FnOnce(&mut serde_yaml::Value)) {
    let path = config_dir.join("cluster.yaml");
    let mut config: serde_yaml::Value = serde_yaml::from_slice(&fs::read(&path).unwrap()).unwrap();
    edit(&mut config);
    fs::write(path, serde_yaml::to_string(&config).unwrap()).unwrap();
}

fn bindings_nearest(server: &TestServer, expected: &str) {
    let response = graph_http_client().post(format!("{}/graphs/bindings/query", server.base_url))
        .bearer_auth("live-deployment-token")
        .timeout(std::time::Duration::from_secs(10))
        .json(&serde_json::json!({"query":"query nearest_docs($q: String) { match { $d: SearchDoc } return { $d.name } order { nearest($d.embedding, $q) } limit 1 }", "name":"nearest_docs", "params":{"q":"same-query-text"}}))
        .send().unwrap();
    let status = response.status();
    let body: serde_json::Value = response.json().unwrap();
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["row_count"], 1, "{body}");
    assert_eq!(body["rows"][0]["d.name"], expected, "{body}");
}

fn bindings_external_load(server: &TestServer, uri: &str, allowed: bool) {
    let response = graph_http_client().post(format!("{}/graphs/bindings/load", server.base_url))
        .bearer_auth("live-deployment-token")
        .timeout(std::time::Duration::from_secs(10))
        .json(&serde_json::json!({"mode":"overwrite", "data":serde_json::json!({"type":"Asset", "data":{"name":"kept","content":uri}}).to_string()}))
        .send().unwrap();
    let status = response.status();
    let body: serde_json::Value = response.json().unwrap();
    assert_eq!(status.as_u16(), if allowed { 200 } else { 400 }, "{body}");
    if !allowed {
        assert!(
            body["error"].as_str().unwrap().contains("external Blob"),
            "{body}"
        );
    }
}

fn bindings_external_redirect(server: &TestServer, uri: &str) {
    let client = reqwest::blocking::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap();
    let response = client
        .get(format!("{}/graphs/bindings/blob", server.base_url))
        .header(
            omnigraph_api_types::HTTP_API_CONTRACT_HEADER,
            omnigraph_api_types::HTTP_API_CONTRACT,
        )
        .bearer_auth("live-deployment-token")
        .query(&[
            ("entity", "node"),
            ("type", "Asset"),
            ("id", "kept"),
            ("property", "content"),
        ])
        .send()
        .unwrap();
    assert_eq!(response.status(), 302);
    assert_eq!(response.headers()[reqwest::header::LOCATION], uri);
}
