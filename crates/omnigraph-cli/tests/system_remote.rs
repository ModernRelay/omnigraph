mod support;

use std::fs;

use reqwest::blocking::Client;
use serde_json::json;

use support::*;

#[test]
#[ignore = "requires loopback socket permissions in sandboxed runners"]
fn remote_server_and_cli_end_to_end_flow() {
    let repo = SystemRepo::loaded();
    let server = repo.spawn_server();
    let config = repo.write_config("omnigraph.yaml", &remote_yaml_config(&server.base_url));
    let mutation_file = repo.write_query(
        "system-remote-change.gq",
        r#"
query insert_person($name: String, $age: I32) {
    insert Person { name: $name, age: $age }
}
"#,
    );
    let client = Client::new();

    let health = client
        .get(format!("{}/healthz", server.base_url))
        .send()
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<serde_json::Value>()
        .unwrap();
    assert_eq!(health["status"], "ok");

    let local_snapshot = parse_stdout_json(&output_success(
        cli().arg("snapshot").arg(repo.path()).arg("--json"),
    ));
    let snapshot = parse_stdout_json(&output_success(
        cli()
            .arg("snapshot")
            .arg("--config")
            .arg(&config)
            .arg("--json"),
    ));
    assert_eq!(snapshot["branch"], "main");
    assert_eq!(snapshot["tables"], local_snapshot["tables"]);

    let local_read = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg(repo.path())
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("--name")
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"Alice"}"#)
            .arg("--json"),
    ));
    let read_payload = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg("--config")
            .arg(&config)
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("--name")
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"Alice"}"#)
            .arg("--json"),
    ));
    assert_eq!(read_payload, local_read);
    assert_eq!(read_payload["row_count"], 1);
    assert_eq!(read_payload["rows"][0]["p.name"], "Alice");

    let change_payload = parse_stdout_json(&output_success(
        cli()
            .arg("change")
            .arg("--config")
            .arg(&config)
            .arg("--query")
            .arg(&mutation_file)
            .arg("--params")
            .arg(r#"{"name":"Mina","age":28}"#)
            .arg("--json"),
    ));
    assert_eq!(change_payload["affected_nodes"], 1);

    let query_source = fs::read_to_string(fixture("test.gq")).unwrap();
    let http_read = client
        .post(format!("{}/read", server.base_url))
        .json(&json!({
            "branch": "main",
            "query_source": query_source,
            "query_name": "get_person",
            "params": { "name": "Mina" }
        }))
        .send()
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<serde_json::Value>()
        .unwrap();
    assert_eq!(http_read["row_count"], 1);
    assert_eq!(http_read["rows"][0]["p.name"], "Mina");

    let local_verify = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg(repo.path())
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("--name")
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"Mina"}"#)
            .arg("--json"),
    ));
    assert_eq!(local_verify["row_count"], 1);
    assert_eq!(local_verify["rows"][0]["p.name"], "Mina");

    let manual_run = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(begin_manual_run(repo.path(), "main"));
    let publish_payload = parse_stdout_json(&output_success(
        cli()
            .arg("run")
            .arg("publish")
            .arg("--config")
            .arg(&config)
            .arg(&manual_run)
            .arg("--json"),
    ));
    assert_eq!(publish_payload["run_id"], manual_run);
    assert_eq!(publish_payload["status"], "published");

    let runs_payload = parse_stdout_json(&output_success(
        cli()
            .arg("run")
            .arg("list")
            .arg("--config")
            .arg(&config)
            .arg("--json"),
    ));
    assert!(runs_payload["runs"].as_array().unwrap().len() >= 2);
}
