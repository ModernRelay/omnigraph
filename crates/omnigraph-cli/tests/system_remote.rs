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

const GRAPH_LIST_SERVER_POLICY_YAML: &str = r#"
version: 1
groups:
  admins: [act-admin]
rules:
  - id: admins-can-list-graphs
    allow:
      actors: { group: admins }
      actions: [graph_list]
"#;

fn yaml_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn remote_policy_server_config(graph: &SystemGraph) -> String {
    format!(
        "\
project:
  name: remote-policy-e2e
graphs:
  local:
    uri: {}
    policy:
      file: ./policy.yaml
server:
  graph: local
",
        yaml_string(&graph.path().to_string_lossy())
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
    let graph = SystemGraph::loaded();
    let server = graph.spawn_server();
    let config = graph.write_config("omnigraph.yaml", &remote_yaml_config(&server.base_url));
    let mutation_file = graph.write_query(
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
        cli().arg("snapshot").arg(graph.path()).arg("--json"),
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
            .arg(graph.path())
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
            .arg(graph.path())
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

    // CLI `-e` over the HTTP transport (--config points at remote server).
    // Confirms inline source survives the remote-execution path identically
    // to file-based queries, and exercises `POST /query` end-to-end via the
    // change-then-read round trip we just established.
    let inline_remote_read = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg("--config")
            .arg(&config)
            .arg("-e")
            .arg("query find($name: String) { match { $p: Person { name: $name } } return { $p.name, $p.age } }")
            .arg("--params")
            .arg(r#"{"name":"Mina"}"#)
            .arg("--json"),
    ));
    assert_eq!(inline_remote_read["row_count"], 1);
    assert_eq!(inline_remote_read["rows"][0]["p.name"], "Mina");

    let inline_remote_change = parse_stdout_json(&output_success(
        cli()
            .arg("change")
            .arg("--config")
            .arg(&config)
            .arg("--query-string")
            .arg("query add($name: String, $age: I32) { insert Person { name: $name, age: $age } }")
            .arg("--params")
            .arg(r#"{"name":"Inline","age":42}"#)
            .arg("--json"),
    ));
    assert_eq!(inline_remote_change["affected_nodes"], 1);

    // `POST /query` happy path directly: a hand-rolled HTTP body using the
    // new clean field names.
    let http_query = client
        .post(format!("{}/query", server.base_url))
        .json(&json!({
            "branch": "main",
            "query": "query find($name: String) { match { $p: Person { name: $name } } return { $p.name } }",
            "params": { "name": "Inline" }
        }))
        .send()
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<serde_json::Value>()
        .unwrap();
    assert_eq!(http_query["row_count"], 1);
    assert_eq!(http_query["rows"][0]["p.name"], "Inline");

    // `POST /query` rejects mutations with 400.
    let http_query_mutation = client
        .post(format!("{}/query", server.base_url))
        .json(&json!({
            "branch": "main",
            "query": "query bad($name: String, $age: I32) { insert Person { name: $name, age: $age } }",
            "params": { "name": "Nope", "age": 1 }
        }))
        .send()
        .unwrap();
    assert_eq!(http_query_mutation.status(), reqwest::StatusCode::BAD_REQUEST);

    // `run publish` / `run list` removed. Direct-to-target writes
    // already landed via the change call above; the commit graph is now
    // the audit surface (verified separately by `commit list`).
}

#[test]
#[ignore = "requires loopback socket permissions in sandboxed runners"]
fn remote_schema_apply_via_cli_updates_graph() {
    let graph = SystemGraph::initialized();
    let server = graph.spawn_server();
    let config = graph.write_config("omnigraph.yaml", &remote_yaml_config(&server.base_url));
    let next_schema = graph.write_file(
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
        .block_on(Omnigraph::open(graph.path().to_string_lossy().as_ref()))
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
    let graph = SystemGraph::initialized();
    let server = graph.spawn_server();
    let config = graph.write_config("omnigraph.yaml", &remote_yaml_config(&server.base_url));
    let breaking_schema = graph.write_file(
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
    let graph = SystemGraph::initialized();
    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--from")
            .arg("main")
            .arg("--uri")
            .arg(graph.path())
            .arg("feature"),
    );
    let server = graph.spawn_server();
    let config = graph.write_config("omnigraph.yaml", &remote_yaml_config(&server.base_url));
    let next_schema = graph.write_file(
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
    assert!(stderr.contains("schema apply requires a graph with only main"));
}

#[test]
#[ignore = "requires loopback socket permissions in sandboxed runners"]
fn remote_read_preserves_projection_order_in_json_and_csv() {
    let graph = SystemGraph::loaded();
    let server = graph.spawn_server();
    let config = graph.write_config("omnigraph.yaml", &remote_yaml_config(&server.base_url));
    let ordered_query = graph.write_query(
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
    let graph = SystemGraph::loaded();
    let server = graph.spawn_server();
    let config = graph.write_config("omnigraph.yaml", &remote_yaml_config(&server.base_url));
    let mutation_file = graph.write_query(
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
    let graph = SystemGraph::loaded();
    let server = graph.spawn_server();
    let config = graph.write_config("omnigraph.yaml", &remote_yaml_config(&server.base_url));

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
            // Served target is non-local → destructive-confirm gate (RFC-011 D9).
            .arg("--yes")
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
    let graph = SystemGraph::loaded();
    let server = graph.spawn_server();
    let config = graph.write_config("omnigraph.yaml", &remote_yaml_config(&server.base_url));
    let mutation_file = graph.write_query(
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
    let export_path = graph.write_jsonl("system-remote-exported.jsonl", &exported);
    let imported_graph = graph
        .path()
        .parent()
        .unwrap()
        .join("imported-remote-export.omni");

    output_success(
        cli()
            .arg("init")
            .arg("--schema")
            .arg(fixture("test.pg"))
            .arg(&imported_graph),
    );
    output_success(
        cli()
            .arg("load")
            .arg("--mode")
            .arg("overwrite")
            .arg("--data")
            .arg(&export_path)
            .arg(&imported_graph),
    );

    let snapshot = parse_stdout_json(&output_success(
        cli().arg("snapshot").arg(&imported_graph).arg("--json"),
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
            .arg(&imported_graph)
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
    let graph = SystemGraph::loaded();
    let server = graph.spawn_server();
    let config = graph.write_config("omnigraph.yaml", &remote_yaml_config(&server.base_url));
    let ingest_data = graph.write_jsonl(
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

/// The unified `load` works against remote graphs through the server's
/// `/ingest` endpoint: without `--from` a missing branch is a hard error
/// (no implicit fork), with `--from` it forks like ingest did.
#[test]
#[ignore = "requires loopback socket permissions in sandboxed runners"]
fn remote_load_round_trips_and_requires_from_for_new_branches() {
    let graph = SystemGraph::loaded();
    let server = graph.spawn_server();
    let config = graph.write_config("omnigraph.yaml", &remote_yaml_config(&server.base_url));
    let extra = graph.write_jsonl(
        "system-remote-load.jsonl",
        r#"{"type":"Person","data":{"name":"Zoe","age":33}}"#,
    );

    // Missing branch without --from: refused remotely, nothing created.
    let failure = output_failure(
        cli()
            .arg("load")
            .arg("--config")
            .arg(&config)
            .arg("--mode")
            .arg("merge")
            .arg("--data")
            .arg(&extra)
            .arg("--branch")
            .arg("feature-load"),
    );
    assert!(
        String::from_utf8_lossy(&failure.stderr).contains("feature-load"),
        "error should name the missing branch"
    );

    // With --from, the remote load forks and lands the rows.
    let payload = parse_stdout_json(&output_success(
        cli()
            .arg("load")
            .arg("--config")
            .arg(&config)
            .arg("--mode")
            .arg("merge")
            .arg("--data")
            .arg(&extra)
            .arg("--branch")
            .arg("feature-load")
            .arg("--from")
            .arg("main")
            .arg("--json"),
    ));
    assert_eq!(payload["branch"], "feature-load");
    assert_eq!(payload["base_branch"], "main");
    assert_eq!(payload["branch_created"], true);
    assert_eq!(payload["nodes_loaded"], 1);

    let snapshot = parse_stdout_json(&output_success(
        cli()
            .arg("snapshot")
            .arg("--config")
            .arg(&config)
            .arg("--branch")
            .arg("feature-load")
            .arg("--json"),
    ));
    assert_eq!(snapshot["branch"], "feature-load");
}

#[test]
#[ignore = "requires loopback socket permissions in sandboxed runners"]
fn remote_ingest_reuses_existing_branch_and_merges_updates() {
    let graph = SystemGraph::loaded();
    let server = graph.spawn_server();
    let config = graph.write_config("omnigraph.yaml", &remote_yaml_config(&server.base_url));

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

    let ingest_data = graph.write_jsonl(
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
    let graph = SystemGraph::loaded();
    let server_config =
        graph.write_config("server-policy.yaml", &remote_policy_server_config(&graph));
    graph.write_config("policy.yaml", REMOTE_POLICY_E2E_YAML);
    let server = graph.spawn_server_with_config_env(
        &server_config,
        &[(
            "OMNIGRAPH_SERVER_BEARER_TOKENS_JSON",
            r#"{"act-bruno":"team-token","act-ragnor":"admin-token"}"#,
        )],
    );
    let client_config = graph.write_config(
        "omnigraph-policy.yaml",
        &remote_policy_client_config(&server.base_url),
    );
    graph.write_config(".env.omni", "POLICY_TEST_TOKEN=team-token\n");
    let mutation_file = graph.write_query(
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

// ─── MR-668 PR 8 — omnigraph graphs list end-to-end ────────────────────────

/// Multi-graph server + CLI `omnigraph graphs list` end-to-end.
///
/// Steps:
///   1. Init a graph `alpha` on disk and write an `omnigraph.yaml`
///      whose `graphs:` map references it.
///   2. Spawn the server with `--config <yaml>`.
///   3. `omnigraph graphs list` — expect to see `alpha`.
///
/// Ignored by default — spawning servers needs loopback socket
/// permissions some sandboxes lack.
#[test]
#[ignore = "requires loopback socket permissions in sandboxed runners"]
fn graphs_list_against_multi_graph_server() {
    let cfg_dir = tempfile::tempdir().unwrap();
    let schema_path = fixture("test.pg");

    // Init `alpha` on disk.
    let alpha_uri = cfg_dir.path().join("alpha.omni");
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        Omnigraph::init(
            alpha_uri.to_str().unwrap(),
            &fs::read_to_string(&schema_path).unwrap(),
        )
        .await
        .unwrap();
    });

    fs::write(
        cfg_dir.path().join("server-policy.yaml"),
        GRAPH_LIST_SERVER_POLICY_YAML,
    )
    .unwrap();

    // Server config with `graphs:` map and no `server.graph` selector
    // — multi mode (rule 4 of the inference matrix). `GET /graphs` is a
    // server-scoped action, so the success path needs an explicit server
    // policy and bearer token.
    let server_config_path = cfg_dir.path().join("omnigraph.yaml");
    fs::write(
        &server_config_path,
        format!(
            "\
server:
  policy:
    file: ./server-policy.yaml
graphs:
  alpha:
    uri: {}
",
            yaml_string(&alpha_uri.to_string_lossy())
        ),
    )
    .unwrap();

    let server = spawn_server_with_config_env(
        &server_config_path,
        &[(
            "OMNIGRAPH_SERVER_BEARER_TOKENS_JSON",
            r#"{"act-admin":"admin-token"}"#,
        )],
    );

    // Client config — the CLI's `--target dev` resolves to `server.base_url`.
    let client_config_path = cfg_dir.path().join("client.yaml");
    fs::write(
        &client_config_path,
        format!(
            "\
graphs:
  dev:
    uri: {}
    bearer_token_env: GRAPH_LIST_TOKEN
cli:
  graph: dev
auth:
  env_file: ./.env.omni
",
            yaml_string(&server.base_url)
        ),
    )
    .unwrap();
    fs::write(
        cfg_dir.path().join(".env.omni"),
        "GRAPH_LIST_TOKEN=admin-token\n",
    )
    .unwrap();

    // `graphs list` lists `alpha`.
    let payload = parse_stdout_json(&output_success(
        cli()
            .arg("graphs")
            .arg("list")
            .arg("--config")
            .arg(&client_config_path)
            .arg("--json"),
    ));
    let ids: Vec<&str> = payload["graphs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|g| g["graph_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["alpha"]);

    drop(server);
}
