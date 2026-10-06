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

    let server = spawn_server_with_cluster_env(
        temp.path(),
        &[(
            "OMNIGRAPH_SERVER_BEARER_TOKENS_JSON",
            r#"{"act-live":"live-deployment-token"}"#,
        )],
    );
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
        // A true out-of-band writer changes the graph without advancing the
        // ledger. Desired schema edits alone must not authorize that drift.
        let observed = tokio::runtime::Runtime::new().unwrap().block_on(async {
            let db = Omnigraph::open(temp.path().join("graphs/knowledge.omni").to_str().unwrap())
                .await
                .unwrap();
            db.apply_schema("node Person { name: String @key age: I32? bypassed: Bool? }\n")
                .await
                .unwrap();
            db.schema_contract_digest()
        });
        let before_refusal = fs::read(temp.path().join("__cluster/state.json")).unwrap();
        let refused = output_failure(
            cli()
                .args(["cluster", "apply", "--config"])
                .arg(temp.path())
                .arg("--json"),
        );
        assert_eq!(
            parse_stdout_json(&refused)["diagnostics"][0]["code"],
            "applied_schema_drift"
        );
        let correction = temp.path().join("schema-correction.json");
        for oversized in [false, true] {
            if oversized {
                fs::File::create(&correction)
                    .unwrap()
                    .set_len(omnigraph_cluster::MAX_BUNDLE_BYTES as u64 + 1)
                    .unwrap();
            } else {
                fs::write(&correction, "{").unwrap();
            }
            let output = output_failure(
                cli()
                    .args(["cluster", "apply", "--config"])
                    .arg(temp.path())
                    .arg("--schema-correction")
                    .arg(&correction)
                    .arg("--json"),
            );
            assert!(
                String::from_utf8_lossy(&output.stderr).contains(if oversized {
                    "deployment input limit"
                } else {
                    "invalid schema correction JSON"
                }),
                "{output:?}"
            );
            assert_eq!(
                fs::read(temp.path().join("__cluster/state.json")).unwrap(),
                before_refusal
            );
            assert!(!temp.path().join("__cluster/lock.json").exists());
        }
        fs::write(
            &correction,
            serde_json::to_vec(&serde_json::json!({"knowledge": observed})).unwrap(),
        )
        .unwrap();
        let output = output_success(
            cli()
                .args(["--as", "operator:deploy", "cluster", "apply", "--config"])
                .arg(temp.path())
                .arg("--schema-correction")
                .arg(&correction)
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
        fs::remove_file(&correction).unwrap();
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
        assert!(
            String::from_utf8_lossy(&refused.stderr).contains("requires config-addressed apply")
        );
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
        String::from_utf8_lossy(&refused.stderr).contains("already exists"),
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
fn cluster_e2e_declared_graph_created_by_apply_and_deletion_refused() {
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
    fs::write(&config, source).unwrap();
    let before = fs::read(temp.path().join("__cluster/state.json")).unwrap();
    let refused = output_failure(
        cli()
            .args(["cluster", "apply", "--config"])
            .arg(temp.path())
            .arg("--json"),
    );
    assert_eq!(
        parse_stdout_json(&refused)["diagnostics"][0]["code"],
        "deployment_scope"
    );
    assert_eq!(
        fs::read(temp.path().join("__cluster/state.json")).unwrap(),
        before
    );
    assert!(
        temp.path()
            .join("graphs/engineering.omni/__manifest")
            .exists()
    );
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
fn cluster_e2e_payload_drift_self_heals() {
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
    assert_eq!(fs::read(&blob).unwrap(), expected);
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
