mod support;

use std::fs;

use omnigraph::db::Omnigraph;
use reqwest::blocking::Client;
use serde_json::json;

use support::*;

const REMOTE_POLICY_E2E_YAML: &str = r#"
version: 1
groups:
  team: [act-bruno]
  admins: [act-ragnor]
protected_branches: [main]
rules:
  - id: team-read
    allow:
      actors: { group: team }
      actions: [read]
      branch_scope: any
  - id: team-branch-create
    allow:
      actors: { group: team }
      actions: [branch_create]
      target_branch_scope: unprotected
  - id: team-write-unprotected
    allow:
      actors: { group: team }
      actions: [change]
      branch_scope: unprotected
  - id: admins-promote
    allow:
      actors: { group: admins }
      actions: [branch_merge]
      target_branch_scope: protected
"#;

fn yaml_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn remote_policy_server_config(repo: &SystemRepo) -> String {
    format!(
        "\
project:
  name: remote-policy-e2e
graphs:
  local:
    uri: {}
server:
  graph: local
policy:
  file: ./policy.yaml
",
        yaml_string(&repo.path().to_string_lossy())
    )
}

fn remote_policy_client_config(url: &str) -> String {
    format!(
        "\
graphs:
  dev:
    uri: {}
    bearer_token_env: POLICY_TEST_TOKEN
cli:
  graph: dev
  branch: main
query:
  roots:
    - .
auth:
  env_file: ./.env.omni
",
        yaml_string(url)
    )
}

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

    // MR-771: `run publish` / `run list` removed. Direct-to-target writes
    // already landed via the change call above; the commit graph is now
    // the audit surface (verified separately by `commit list`).
}

#[test]
#[ignore = "requires loopback socket permissions in sandboxed runners"]
fn remote_schema_apply_via_cli_updates_repo() {
    let repo = SystemRepo::initialized();
    let server = repo.spawn_server();
    let config = repo.write_config("omnigraph.yaml", &remote_yaml_config(&server.base_url));
    let next_schema = repo.write_file(
        "next.pg",
        &fs::read_to_string(fixture("test.pg")).unwrap().replace(
            "    age: I32?\n}",
            "    age: I32?\n    nickname: String?\n}",
        ),
    );

    let payload = parse_stdout_json(&output_success(
        cli()
            .arg("schema")
            .arg("apply")
            .arg("--config")
            .arg(&config)
            .arg("--schema")
            .arg(&next_schema)
            .arg("--json"),
    ));
    assert_eq!(payload["applied"], true);

    let db = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(Omnigraph::open(repo.path().to_string_lossy().as_ref()))
        .unwrap();
    assert!(
        db.catalog().node_types["Person"]
            .properties
            .contains_key("nickname")
    );
}

#[test]
#[ignore = "requires loopback socket permissions in sandboxed runners"]
fn remote_schema_apply_rejects_unsupported_plan() {
    let repo = SystemRepo::initialized();
    let server = repo.spawn_server();
    let config = repo.write_config("omnigraph.yaml", &remote_yaml_config(&server.base_url));
    let breaking_schema = repo.write_file(
        "breaking.pg",
        &fs::read_to_string(fixture("test.pg"))
            .unwrap()
            .replace("age: I32?", "age: I64?"),
    );

    let output = output_failure(
        cli()
            .arg("schema")
            .arg("apply")
            .arg("--config")
            .arg(&config)
            .arg("--schema")
            .arg(&breaking_schema),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("changing property type"));
}

#[test]
#[ignore = "requires loopback socket permissions in sandboxed runners"]
fn remote_schema_apply_rejects_when_non_main_branch_exists() {
    let repo = SystemRepo::initialized();
    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--from")
            .arg("main")
            .arg("--uri")
            .arg(repo.path())
            .arg("feature"),
    );
    let server = repo.spawn_server();
    let config = repo.write_config("omnigraph.yaml", &remote_yaml_config(&server.base_url));
    let next_schema = repo.write_file(
        "next.pg",
        &fs::read_to_string(fixture("test.pg")).unwrap().replace(
            "    age: I32?\n}",
            "    age: I32?\n    nickname: String?\n}",
        ),
    );

    let output = output_failure(
        cli()
            .arg("schema")
            .arg("apply")
            .arg("--config")
            .arg(&config)
            .arg("--schema")
            .arg(&next_schema),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("schema apply requires a repo with only main"));
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
fn remote_branch_delete_removes_branch() {
    let repo = SystemRepo::loaded();
    let server = repo.spawn_server();
    let config = repo.write_config("omnigraph.yaml", &remote_yaml_config(&server.base_url));

    parse_stdout_json(&output_success(
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

    let deleted = parse_stdout_json(&output_success(
        cli()
            .arg("branch")
            .arg("delete")
            .arg("--config")
            .arg(&config)
            .arg("feature")
            .arg("--json"),
    ));
    assert_eq!(deleted["name"], "feature");

    let listed = parse_stdout_json(&output_success(
        cli()
            .arg("branch")
            .arg("list")
            .arg("--config")
            .arg(&config)
            .arg("--json"),
    ));
    assert_eq!(listed["branches"], json!(["main"]));
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
    let imported_repo = repo
        .path()
        .parent()
        .unwrap()
        .join("imported-remote-export.omni");

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
        cli().arg("snapshot").arg(&imported_repo).arg("--json"),
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

#[test]
#[ignore = "requires loopback socket permissions in sandboxed runners"]
fn remote_ingest_creates_review_branch_and_keeps_it_readable() {
    let repo = SystemRepo::loaded();
    let server = repo.spawn_server();
    let config = repo.write_config("omnigraph.yaml", &remote_yaml_config(&server.base_url));
    let ingest_data = repo.write_jsonl(
        "system-remote-ingest.jsonl",
        r#"{"type":"Person","data":{"name":"Zoe","age":33}}
{"type":"Person","data":{"name":"Bob","age":26}}"#,
    );

    let ingest_payload = parse_stdout_json(&output_success(
        cli()
            .arg("ingest")
            .arg("--config")
            .arg(&config)
            .arg("--data")
            .arg(&ingest_data)
            .arg("--branch")
            .arg("feature-ingest")
            .arg("--json"),
    ));
    assert_eq!(ingest_payload["branch"], "feature-ingest");
    assert_eq!(ingest_payload["base_branch"], "main");
    assert_eq!(ingest_payload["branch_created"], true);
    assert_eq!(ingest_payload["mode"], "merge");
    assert_eq!(ingest_payload["tables"][0]["table_key"], "node:Person");
    assert_eq!(ingest_payload["tables"][0]["rows_loaded"], 2);

    let feature_snapshot = parse_stdout_json(&output_success(
        cli()
            .arg("snapshot")
            .arg("--config")
            .arg(&config)
            .arg("--branch")
            .arg("feature-ingest")
            .arg("--json"),
    ));
    assert_eq!(feature_snapshot["branch"], "feature-ingest");

    let zoe = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg("--config")
            .arg(&config)
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("--name")
            .arg("get_person")
            .arg("--branch")
            .arg("feature-ingest")
            .arg("--params")
            .arg(r#"{"name":"Zoe"}"#)
            .arg("--json"),
    ));
    assert_eq!(zoe["row_count"], 1);
    assert_eq!(zoe["rows"][0]["p.name"], "Zoe");
}

#[test]
#[ignore = "requires loopback socket permissions in sandboxed runners"]
fn remote_ingest_reuses_existing_branch_and_merges_updates() {
    let repo = SystemRepo::loaded();
    let server = repo.spawn_server();
    let config = repo.write_config("omnigraph.yaml", &remote_yaml_config(&server.base_url));

    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--config")
            .arg(&config)
            .arg("--from")
            .arg("main")
            .arg("feature-ingest"),
    );

    let ingest_data = repo.write_jsonl(
        "system-remote-ingest-merge.jsonl",
        r#"{"type":"Person","data":{"name":"Bob","age":26}}
{"type":"Person","data":{"name":"Zoe","age":33}}"#,
    );

    let ingest_payload = parse_stdout_json(&output_success(
        cli()
            .arg("ingest")
            .arg("--config")
            .arg(&config)
            .arg("--data")
            .arg(&ingest_data)
            .arg("--branch")
            .arg("feature-ingest")
            .arg("--from")
            .arg("missing-base")
            .arg("--json"),
    ));
    assert_eq!(ingest_payload["branch"], "feature-ingest");
    assert_eq!(ingest_payload["base_branch"], "missing-base");
    assert_eq!(ingest_payload["branch_created"], false);
    assert_eq!(ingest_payload["mode"], "merge");
    assert_eq!(ingest_payload["tables"][0]["table_key"], "node:Person");
    assert_eq!(ingest_payload["tables"][0]["rows_loaded"], 2);

    let bob = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg("--config")
            .arg(&config)
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("--name")
            .arg("get_person")
            .arg("--branch")
            .arg("feature-ingest")
            .arg("--params")
            .arg(r#"{"name":"Bob"}"#)
            .arg("--json"),
    ));
    assert_eq!(bob["row_count"], 1);
    assert_eq!(bob["rows"][0]["p.age"], 26);

    let zoe = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg("--config")
            .arg(&config)
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("--name")
            .arg("get_person")
            .arg("--branch")
            .arg("feature-ingest")
            .arg("--params")
            .arg(r#"{"name":"Zoe"}"#)
            .arg("--json"),
    ));
    assert_eq!(zoe["row_count"], 1);
    assert_eq!(zoe["rows"][0]["p.name"], "Zoe");
}

#[test]
#[ignore = "requires loopback socket permissions in sandboxed runners"]
fn remote_policy_enforces_branch_first_cli_workflow() {
    let repo = SystemRepo::loaded();
    let server_config =
        repo.write_config("server-policy.yaml", &remote_policy_server_config(&repo));
    repo.write_config("policy.yaml", REMOTE_POLICY_E2E_YAML);
    let server = repo.spawn_server_with_config_env(
        &server_config,
        &[(
            "OMNIGRAPH_SERVER_BEARER_TOKENS_JSON",
            r#"{"act-bruno":"team-token","act-ragnor":"admin-token"}"#,
        )],
    );
    let client_config = repo.write_config(
        "omnigraph-policy.yaml",
        &remote_policy_client_config(&server.base_url),
    );
    repo.write_config(".env.omni", "POLICY_TEST_TOKEN=team-token\n");
    let mutation_file = repo.write_query(
        "system-remote-policy-change.gq",
        r#"
query insert_person($name: String, $age: I32) {
    insert Person { name: $name, age: $age }
}
"#,
    );

    let snapshot = parse_stdout_json(&output_success(
        cli()
            .arg("snapshot")
            .arg("--config")
            .arg(&client_config)
            .arg("--json"),
    ));
    assert_eq!(snapshot["branch"], "main");

    let denied_main_change = output_failure(
        cli()
            .arg("change")
            .arg("--config")
            .arg(&client_config)
            .arg("--query")
            .arg(&mutation_file)
            .arg("--params")
            .arg(r#"{"name":"PolicyRemote","age":41}"#)
            .arg("--json"),
    );
    let denied_main_stderr = String::from_utf8(denied_main_change.stderr).unwrap();
    assert!(denied_main_stderr.contains("policy denied action 'change' on branch 'main'"));

    let created = parse_stdout_json(&output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--config")
            .arg(&client_config)
            .arg("--from")
            .arg("main")
            .arg("feature")
            .arg("--json"),
    ));
    assert_eq!(created["name"], "feature");

    let changed = parse_stdout_json(&output_success(
        cli()
            .arg("change")
            .arg("--config")
            .arg(&client_config)
            .arg("--query")
            .arg(&mutation_file)
            .arg("--branch")
            .arg("feature")
            .arg("--params")
            .arg(r#"{"name":"PolicyRemote","age":41}"#)
            .arg("--json"),
    ));
    assert_eq!(changed["branch"], "feature");
    assert_eq!(changed["affected_nodes"], 1);

    let denied_merge = output_failure(
        cli()
            .arg("branch")
            .arg("merge")
            .arg("--config")
            .arg(&client_config)
            .arg("feature")
            .arg("--into")
            .arg("main")
            .arg("--json"),
    );
    let denied_merge_stderr = String::from_utf8(denied_merge.stderr).unwrap();
    assert!(denied_merge_stderr.contains("policy denied action 'branch_merge'"));

    let merged = parse_stdout_json(&output_success(
        cli()
            .env("POLICY_TEST_TOKEN", "admin-token")
            .arg("branch")
            .arg("merge")
            .arg("--config")
            .arg(&client_config)
            .arg("feature")
            .arg("--into")
            .arg("main")
            .arg("--json"),
    ));
    assert_eq!(merged["target"], "main");

    let verify = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg("--config")
            .arg(&client_config)
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("--name")
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"PolicyRemote"}"#)
            .arg("--json"),
    ));
    assert_eq!(verify["row_count"], 1);
    assert_eq!(verify["rows"][0]["p.name"], "PolicyRemote");
}
