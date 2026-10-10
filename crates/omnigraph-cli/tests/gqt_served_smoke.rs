mod support;

use std::fs;
use std::path::Path;
use std::time::Duration;

use serde_json::Value;

#[test]
#[ignore = "environment: prebuilt omnigraph-gqt and omnigraph-server binaries"]
fn gqt_server_binary_round_trip() {
    let binaries = Path::new(env!("CARGO_BIN_EXE_omnigraph")).parent().unwrap();
    let gqt = binaries.join(format!("omnigraph-gqt{}", std::env::consts::EXE_SUFFIX));
    let server_binary = binaries.join(format!("omnigraph-server{}", std::env::consts::EXE_SUFFIX));
    assert!(gqt.is_file(), "build the GQT binary before this test");
    assert!(
        server_binary.is_file(),
        "build the server binary before this test"
    );
    let case = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../omnigraph-gqt/cases/camelcase_property_in_mutation_predicate.gqt");
    let source = fs::read_to_string(&case).unwrap();
    let schema = source
        .split_once("--- schema\n")
        .unwrap()
        .1
        .split_once("\n--- seed\n")
        .unwrap()
        .0;
    let cluster = tempfile::tempdir().unwrap();
    fs::write(cluster.path().join("schema.pg"), schema).unwrap();
    fs::write(
        cluster.path().join("smoke.policy.yaml"),
        r#"version: 1
groups:
  smoke: [act-gqt-smoke]
rules:
  - id: fixture-read-write
    allow:
      actors: { group: smoke }
      actions: [read, change]
      branch_scope: any
"#,
    )
    .unwrap();
    fs::write(
        cluster.path().join("cluster.yaml"),
        "version: 1\nmetadata:\n  name: gqt-smoke\nstate:\n  backend: cluster\n  lock: true\ngraphs:\n  smoke:\n    schema: ./schema.pg\npolicies:\n  smoke:\n    file: ./smoke.policy.yaml\n    applies_to: [smoke]\n",
    )
    .unwrap();
    support::apply_cluster_fixture(cluster.path());
    let token = "gqt-smoke-token-must-not-be-retained";
    let tokens = serde_json::json!({"act-gqt-smoke": token}).to_string();
    let server = support::spawn_server_with_cluster_binary_env(
        cluster.path(),
        &server_binary,
        &[("OMNIGRAPH_SERVER_BEARER_TOKENS_JSON", &tokens)],
    );
    let artifacts = tempfile::tempdir().unwrap();
    let mut command = assert_cmd::Command::new(&gqt);
    command
        .env_clear()
        .env("OMNIGRAPH_HOME", cluster.path().join("operator-home"))
        .arg(&case)
        .args([
            "--server",
            &server.base_url,
            "--graph",
            "smoke",
            "--token",
            token,
            "--artifacts",
        ])
        .arg(artifacts.path())
        .timeout(Duration::from_secs(30));
    let assertion = command.assert().success();
    let output = assertion.get_output();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let report_path = stdout
        .lines()
        .find_map(|line| line.strip_prefix("GQT report: "))
        .expect("command must retain its invocation report");
    let report_text = fs::read_to_string(report_path).unwrap();
    for text in [
        stdout.as_ref(),
        String::from_utf8_lossy(&output.stderr).as_ref(),
        &report_text,
    ] {
        assert!(
            !text.contains(token),
            "bearer token retained in command output"
        );
    }
    let report: Value = serde_json::from_str(&report_text).unwrap();
    assert_eq!(report["result"], serde_json::json!({"Ok": null}));
    let attempts = report["attempts"].as_array().unwrap();
    assert_eq!(attempts.len(), 1, "served execution must not replay");
    let attempt = &attempts[0];
    assert_eq!(attempt["environment"]["target"], "omnigraph-server");
    assert_eq!(attempt["outcome"]["Ok"]["code"], "passed");
    assert_eq!(attempt["input"]["server"]["graph"], "smoke");
    assert!(attempt["input"]["server"].get("token").is_none());
    assert_eq!(fs::read_to_string(&case).unwrap(), source);
    assert_cmd::Command::new(gqt)
        .env_clear()
        .args(["--replay", report_path])
        .timeout(Duration::from_secs(30))
        .assert()
        .failure()
        .stderr(predicates::str::contains("cannot replay, nor served ones"));
}
