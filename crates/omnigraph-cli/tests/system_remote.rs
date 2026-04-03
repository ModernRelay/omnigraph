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

#[test]
#[ignore = "requires loopback socket permissions in sandboxed runners"]
fn remote_read_preserves_projection_order_in_json_and_csv() {
    let repo = SystemRepo::loaded();
    let server = repo.spawn_server();
    let config = repo.write_config("omnigraph.yaml", &remote_yaml_config(&server.base_url));
    let ordered_query = repo.write_query(
        "ordered-remote.gq",
        r#"
query ordered_person($name: String) {
    match {
        $p: Person { name: $name }
    }
    return { $p.age, $p.name }
}
"#,
    );

    let json_payload = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg("--config")
            .arg(&config)
            .arg("--query")
            .arg(&ordered_query)
            .arg("--name")
            .arg("ordered_person")
            .arg("--params")
            .arg(r#"{"name":"Alice"}"#)
            .arg("--json"),
    ));
    let columns = json_payload["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(columns, vec!["p.age", "p.name"]);

    let csv = stdout_string(&output_success(
        cli()
            .arg("read")
            .arg("--config")
            .arg(&config)
            .arg("--query")
            .arg(&ordered_query)
            .arg("--name")
            .arg("ordered_person")
            .arg("--params")
            .arg(r#"{"name":"Alice"}"#)
            .arg("--format")
            .arg("csv"),
    ));
    let mut lines = csv.lines();
    assert_eq!(lines.next().unwrap(), "p.age,p.name");
    assert_eq!(lines.next().unwrap(), "30,Alice");
}

#[test]
#[ignore = "requires loopback socket permissions in sandboxed runners"]
fn remote_branch_create_list_merge_flow() {
    let repo = SystemRepo::loaded();
    let server = repo.spawn_server();
    let config = repo.write_config("omnigraph.yaml", &remote_yaml_config(&server.base_url));
    let mutation_file = repo.write_query(
        "system-remote-branch-change.gq",
        r#"
query insert_person($name: String, $age: I32) {
    insert Person { name: $name, age: $age }
}
"#,
    );

    let initial = parse_stdout_json(&output_success(
        cli()
            .arg("branch")
            .arg("list")
            .arg("--config")
            .arg(&config)
            .arg("--json"),
    ));
    assert_eq!(initial["branches"], json!(["main"]));

    let created = parse_stdout_json(&output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--config")
            .arg(&config)
            .arg("--from")
            .arg("main")
            .arg("feature")
            .arg("--json"),
    ));
    assert_eq!(created["from"], "main");
    assert_eq!(created["name"], "feature");

    let listed = parse_stdout_json(&output_success(
        cli()
            .arg("branch")
            .arg("list")
            .arg("--config")
            .arg(&config)
            .arg("--json"),
    ));
    assert_eq!(listed["branches"], json!(["feature", "main"]));

    let changed = parse_stdout_json(&output_success(
        cli()
            .arg("change")
            .arg("--config")
            .arg(&config)
            .arg("--query")
            .arg(&mutation_file)
            .arg("--branch")
            .arg("feature")
            .arg("--params")
            .arg(r#"{"name":"Zoe","age":33}"#)
            .arg("--json"),
    ));
    assert_eq!(changed["branch"], "feature");
    assert_eq!(changed["affected_nodes"], 1);

    let merged = parse_stdout_json(&output_success(
        cli()
            .arg("branch")
            .arg("merge")
            .arg("--config")
            .arg(&config)
            .arg("feature")
            .arg("--into")
            .arg("main")
            .arg("--json"),
    ));
    assert_eq!(merged["source"], "feature");
    assert_eq!(merged["target"], "main");
    assert_eq!(merged["outcome"], "fast_forward");

    let verify = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg("--config")
            .arg(&config)
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("--name")
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"Zoe"}"#)
            .arg("--json"),
    ));
    assert_eq!(verify["row_count"], 1);
    assert_eq!(verify["rows"][0]["p.name"], "Zoe");
}

#[test]
#[ignore = "requires loopback socket permissions in sandboxed runners"]
fn remote_export_round_trips_full_branch_graph() {
    let repo = SystemRepo::loaded();
    let server = repo.spawn_server();
    let config = repo.write_config("omnigraph.yaml", &remote_yaml_config(&server.base_url));
    let mutation_file = repo.write_query(
        "system-remote-export-change.gq",
        r#"
query insert_person($name: String, $age: I32) {
    insert Person { name: $name, age: $age }
}

query add_friend($from: String, $to: String) {
    insert Knows { from: $from, to: $to }
}
"#,
    );

    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--config")
            .arg(&config)
            .arg("--from")
            .arg("main")
            .arg("feature"),
    );

    output_success(
        cli()
            .arg("change")
            .arg("--config")
            .arg(&config)
            .arg("--query")
            .arg(&mutation_file)
            .arg("--name")
            .arg("insert_person")
            .arg("--branch")
            .arg("feature")
            .arg("--params")
            .arg(r#"{"name":"Eve","age":29}"#)
            .arg("--json"),
    );
    output_success(
        cli()
            .arg("change")
            .arg("--config")
            .arg(&config)
            .arg("--query")
            .arg(&mutation_file)
            .arg("--name")
            .arg("add_friend")
            .arg("--branch")
            .arg("feature")
            .arg("--params")
            .arg(r#"{"from":"Alice","to":"Eve"}"#)
            .arg("--json"),
    );

    let exported = stdout_string(&output_success(
        cli()
            .arg("export")
            .arg("--config")
            .arg(&config)
            .arg("--branch")
            .arg("feature")
            .arg("--jsonl"),
    ));
    let export_path = repo.write_jsonl("system-remote-exported.jsonl", &exported);
    let imported_repo = repo.path().parent().unwrap().join("imported-remote-export.omni");

    output_success(
        cli()
            .arg("init")
            .arg("--schema")
            .arg(fixture("test.pg"))
            .arg(&imported_repo),
    );
    output_success(
        cli()
            .arg("load")
            .arg("--data")
            .arg(&export_path)
            .arg(&imported_repo),
    );

    let snapshot = parse_stdout_json(&output_success(
        cli()
            .arg("snapshot")
            .arg(&imported_repo)
            .arg("--json"),
    ));
    assert_eq!(
        snapshot["tables"]
            .as_array()
            .unwrap()
            .iter()
            .find(|table| table["table_key"] == "node:Person")
            .unwrap()["row_count"],
        5
    );
    assert_eq!(
        snapshot["tables"]
            .as_array()
            .unwrap()
            .iter()
            .find(|table| table["table_key"] == "edge:Knows")
            .unwrap()["row_count"],
        4
    );

    let eve = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg(&imported_repo)
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("--name")
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"Eve"}"#)
            .arg("--json"),
    ));
    assert_eq!(eve["row_count"], 1);
    assert_eq!(eve["rows"][0]["p.name"], "Eve");
}
