mod support;

use std::fs;

use omnigraph::db::Omnigraph;
use reqwest::blocking::Client;
use serde_json::json;

use support::*;

/// Graph id every served test addresses (`--server <url> --graph GRAPH_ID`).
/// RFC-011: the server is cluster-only, so a graph selector is always required
/// — even for a single-graph cluster.
const GRAPH_ID: &str = "knowledge";

/// Graph-bound Cedar bundle for the policy-flavored remote tests. `act-bruno`
/// (team) reads + writes unprotected branches; `act-ragnor` (admins) merges
/// into protected `main`.
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

/// Server-scoped bundle granting `act-admin` the `graph_list` action so
/// `GET /graphs` succeeds.
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

#[test]
#[ignore = "requires loopback socket permissions in sandboxed runners"]
fn remote_server_and_cli_end_to_end_flow() {
    let cluster = converged_loaded_cluster(GRAPH_ID, None);
    let server = spawn_server_with_cluster(cluster.path());
    // The served graph's storage root — used for embedded-side cross checks.
    let served_root = cluster
        .path()
        .join("graphs")
        .join(format!("{GRAPH_ID}.omni"));
    let temp = tempfile::tempdir().unwrap();
    let mutation_file = temp.path().join("system-remote-change.gq");
    fs::write(
        &mutation_file,
        r#"
query insert_person($name: String, $age: I32) {
    insert Person { name: $name, age: $age }
}
"#,
    )
    .unwrap();
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
        cli().arg("snapshot").arg(&served_root).arg("--json"),
    ));
    let snapshot = parse_stdout_json(&output_success(
        cli()
            .arg("snapshot")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("--json"),
    ));
    assert_eq!(snapshot["branch"], "main");
    assert_eq!(snapshot["tables"], local_snapshot["tables"]);

    let local_read = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg("--store")
            .arg(&served_root)
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"Alice"}"#)
            .arg("--json"),
    ));
    let read_payload = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"Alice"}"#)
            .arg("--json"),
    ));
    assert_eq!(read_payload, local_read);
    assert_eq!(read_payload["row_count"], 1);
    assert_eq!(read_payload["rows"][0]["p.name"], "Alice");

    // Served write: no `--as` (the server resolves the actor; here the server
    // is `--unauthenticated`, so the actor is the server default).
    let change_payload = parse_stdout_json(&output_success(
        cli()
            .arg("change")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("--query")
            .arg(&mutation_file)
            .arg("--params")
            .arg(r#"{"name":"Mina","age":28}"#)
            .arg("--json"),
    ));
    assert_eq!(change_payload["affected_nodes"], 1);

    let query_source = fs::read_to_string(fixture("test.gq")).unwrap();
    let http_read = client
        .post(format!("{}/graphs/{GRAPH_ID}/read", server.base_url))
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
            .arg("--store")
            .arg(&served_root)
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"Mina"}"#)
            .arg("--json"),
    ));
    assert_eq!(local_verify["row_count"], 1);
    assert_eq!(local_verify["rows"][0]["p.name"], "Mina");

    // CLI inline source over the HTTP transport (--server). Confirms inline
    // source survives the remote-execution path identically to file-based
    // queries.
    let inline_remote_read = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
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
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("--query-string")
            .arg("query add($name: String, $age: I32) { insert Person { name: $name, age: $age } }")
            .arg("--params")
            .arg(r#"{"name":"Inline","age":42}"#)
            .arg("--json"),
    ));
    assert_eq!(inline_remote_change["affected_nodes"], 1);

    // `POST /graphs/{id}/query` happy path directly.
    let http_query = client
        .post(format!("{}/graphs/{GRAPH_ID}/query", server.base_url))
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

    // `POST /graphs/{id}/query` rejects mutations with 400.
    let http_query_mutation = client
        .post(format!("{}/graphs/{GRAPH_ID}/query", server.base_url))
        .json(&json!({
            "branch": "main",
            "query": "query bad($name: String, $age: I32) { insert Person { name: $name, age: $age } }",
            "params": { "name": "Nope", "age": 1 }
        }))
        .send()
        .unwrap();
    assert_eq!(
        http_query_mutation.status(),
        reqwest::StatusCode::BAD_REQUEST
    );
}

#[test]
#[ignore = "requires loopback socket permissions in sandboxed runners"]
fn remote_schema_apply_via_cli_updates_graph() {
    let cluster = converged_loaded_cluster(GRAPH_ID, None);
    let server = spawn_server_with_cluster(cluster.path());
    let served_root = cluster
        .path()
        .join("graphs")
        .join(format!("{GRAPH_ID}.omni"));
    let temp = tempfile::tempdir().unwrap();
    let next_schema = temp.path().join("next.pg");
    fs::write(
        &next_schema,
        fs::read_to_string(fixture("test.pg")).unwrap().replace(
            "    age: I32?\n}",
            "    age: I32?\n    nickname: String?\n}",
        ),
    )
    .unwrap();

    let payload = parse_stdout_json(&output_success(
        cli()
            .arg("schema")
            .arg("apply")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("--schema")
            .arg(&next_schema)
            .arg("--json"),
    ));
    assert_eq!(payload["applied"], true);

    let db = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(Omnigraph::open(served_root.to_string_lossy().as_ref()))
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
    let cluster = converged_loaded_cluster(GRAPH_ID, None);
    let server = spawn_server_with_cluster(cluster.path());
    let temp = tempfile::tempdir().unwrap();
    let breaking_schema = temp.path().join("breaking.pg");
    fs::write(
        &breaking_schema,
        fs::read_to_string(fixture("test.pg"))
            .unwrap()
            .replace("age: I32?", "age: I64?"),
    )
    .unwrap();

    let output = output_failure(
        cli()
            .arg("schema")
            .arg("apply")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("--schema")
            .arg(&breaking_schema),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("changing property type"),
        "expected unsupported-plan error, got: {stderr}"
    );
}

#[test]
#[ignore = "requires loopback socket permissions in sandboxed runners"]
fn remote_schema_apply_rejects_when_non_main_branch_exists() {
    let cluster = converged_loaded_cluster(GRAPH_ID, None);
    let server = spawn_server_with_cluster(cluster.path());

    // Create a non-main branch over the served path so the schema-apply
    // single-branch precondition fails.
    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("--from")
            .arg("main")
            .arg("feature"),
    );

    let temp = tempfile::tempdir().unwrap();
    let next_schema = temp.path().join("next.pg");
    fs::write(
        &next_schema,
        fs::read_to_string(fixture("test.pg")).unwrap().replace(
            "    age: I32?\n}",
            "    age: I32?\n    nickname: String?\n}",
        ),
    )
    .unwrap();

    let output = output_failure(
        cli()
            .arg("schema")
            .arg("apply")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("--schema")
            .arg(&next_schema),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("schema apply requires a graph with only main"),
        "expected single-branch precondition error, got: {stderr}"
    );
}

#[test]
#[ignore = "requires loopback socket permissions in sandboxed runners"]
fn remote_read_preserves_projection_order_in_json_and_csv() {
    let cluster = converged_loaded_cluster(GRAPH_ID, None);
    let server = spawn_server_with_cluster(cluster.path());
    let temp = tempfile::tempdir().unwrap();
    let ordered_query = temp.path().join("ordered-remote.gq");
    fs::write(
        &ordered_query,
        r#"
query ordered_person($name: String) {
    match {
        $p: Person { name: $name }
    }
    return { $p.age, $p.name }
}
"#,
    )
    .unwrap();

    let json_payload = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("--query")
            .arg(&ordered_query)
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
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("--query")
            .arg(&ordered_query)
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
    let cluster = converged_loaded_cluster(GRAPH_ID, None);
    let server = spawn_server_with_cluster(cluster.path());
    let temp = tempfile::tempdir().unwrap();
    let mutation_file = temp.path().join("system-remote-branch-change.gq");
    fs::write(
        &mutation_file,
        r#"
query insert_person($name: String, $age: I32) {
    insert Person { name: $name, age: $age }
}
"#,
    )
    .unwrap();

    let initial = parse_stdout_json(&output_success(
        cli()
            .arg("branch")
            .arg("list")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("--json"),
    ));
    assert_eq!(initial["branches"], json!(["main"]));

    let created = parse_stdout_json(&output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
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
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("--json"),
    ));
    assert_eq!(listed["branches"], json!(["feature", "main"]));

    let changed = parse_stdout_json(&output_success(
        cli()
            .arg("change")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
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
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
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
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("--query")
            .arg(fixture("test.gq"))
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
    let cluster = converged_loaded_cluster(GRAPH_ID, None);
    let server = spawn_server_with_cluster(cluster.path());

    parse_stdout_json(&output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("--from")
            .arg("main")
            .arg("feature")
            .arg("--json"),
    ));

    let deleted = parse_stdout_json(&output_success(
        cli()
            .arg("branch")
            .arg("delete")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
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
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("--json"),
    ));
    assert_eq!(listed["branches"], json!(["main"]));
}

#[test]
#[ignore = "requires loopback socket permissions in sandboxed runners"]
fn remote_export_round_trips_full_branch_graph() {
    let cluster = converged_loaded_cluster(GRAPH_ID, None);
    let server = spawn_server_with_cluster(cluster.path());
    let temp = tempfile::tempdir().unwrap();
    let mutation_file = temp.path().join("system-remote-export-change.gq");
    fs::write(
        &mutation_file,
        r#"
query insert_person($name: String, $age: I32) {
    insert Person { name: $name, age: $age }
}

query add_friend($from: String, $to: String) {
    insert Knows { from: $from, to: $to }
}
"#,
    )
    .unwrap();

    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("--from")
            .arg("main")
            .arg("feature"),
    );

    output_success(
        cli()
            .arg("change")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("--query")
            .arg(&mutation_file)
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
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("--query")
            .arg(&mutation_file)
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
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("--branch")
            .arg("feature")
            .arg("--jsonl"),
    ));
    let export_path = temp.path().join("system-remote-exported.jsonl");
    fs::write(&export_path, &exported).unwrap();
    let imported_graph = temp.path().join("imported-remote-export.omni");

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
            .arg("--store")
            .arg(&imported_graph)
            .arg("--query")
            .arg(fixture("test.gq"))
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
    let cluster = converged_loaded_cluster(GRAPH_ID, None);
    let server = spawn_server_with_cluster(cluster.path());
    let temp = tempfile::tempdir().unwrap();
    let ingest_data = temp.path().join("system-remote-ingest.jsonl");
    fs::write(
        &ingest_data,
        r#"{"type":"Person","data":{"name":"Zoe","age":33}}
{"type":"Person","data":{"name":"Bob","age":26}}"#,
    )
    .unwrap();

    let ingest_payload = parse_stdout_json(&output_success(
        cli()
            .arg("ingest")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
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
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("--branch")
            .arg("feature-ingest")
            .arg("--json"),
    ));
    assert_eq!(feature_snapshot["branch"], "feature-ingest");

    let zoe = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("--query")
            .arg(fixture("test.gq"))
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
    let cluster = converged_loaded_cluster(GRAPH_ID, None);
    let server = spawn_server_with_cluster(cluster.path());
    let temp = tempfile::tempdir().unwrap();
    let extra = temp.path().join("system-remote-load.jsonl");
    fs::write(
        &extra,
        r#"{"type":"Person","data":{"name":"Zoe","age":33}}"#,
    )
    .unwrap();

    // Missing branch without --from: refused remotely, nothing created.
    let failure = output_failure(
        cli()
            .arg("load")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
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
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
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
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("--branch")
            .arg("feature-load")
            .arg("--json"),
    ));
    assert_eq!(snapshot["branch"], "feature-load");
}

#[test]
#[ignore = "requires loopback socket permissions in sandboxed runners"]
fn remote_ingest_reuses_existing_branch_and_merges_updates() {
    let cluster = converged_loaded_cluster(GRAPH_ID, None);
    let server = spawn_server_with_cluster(cluster.path());

    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("--from")
            .arg("main")
            .arg("feature-ingest"),
    );

    let temp = tempfile::tempdir().unwrap();
    let ingest_data = temp.path().join("system-remote-ingest-merge.jsonl");
    fs::write(
        &ingest_data,
        r#"{"type":"Person","data":{"name":"Bob","age":26}}
{"type":"Person","data":{"name":"Zoe","age":33}}"#,
    )
    .unwrap();

    let ingest_payload = parse_stdout_json(&output_success(
        cli()
            .arg("ingest")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
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
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("--query")
            .arg(fixture("test.gq"))
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
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("--query")
            .arg(fixture("test.gq"))
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
    // Served policy enforcement: the cluster binds REMOTE_POLICY_E2E_YAML to the
    // graph, and the server maps bearer tokens to actors. The actor is resolved
    // from the token (no `--as` on served writes).
    let cluster = converged_loaded_cluster(GRAPH_ID, Some(REMOTE_POLICY_E2E_YAML));
    let server = spawn_server_with_cluster_env(
        cluster.path(),
        &[(
            "OMNIGRAPH_SERVER_BEARER_TOKENS_JSON",
            r#"{"act-bruno":"team-token","act-ragnor":"admin-token"}"#,
        )],
    );
    let temp = tempfile::tempdir().unwrap();
    let mutation_file = temp.path().join("system-remote-policy-change.gq");
    fs::write(
        &mutation_file,
        r#"
query insert_person($name: String, $age: I32) {
    insert Person { name: $name, age: $age }
}
"#,
    )
    .unwrap();

    // Reads are granted to the team group (bruno).
    let snapshot = parse_stdout_json(&output_success(
        cli()
            .env("OMNIGRAPH_BEARER_TOKEN", "team-token")
            .arg("snapshot")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("--json"),
    ));
    assert_eq!(snapshot["branch"], "main");

    // bruno cannot change protected main (team-write-unprotected only).
    let denied_main_change = output_failure(
        cli()
            .env("OMNIGRAPH_BEARER_TOKEN", "team-token")
            .arg("change")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("--query")
            .arg(&mutation_file)
            .arg("--params")
            .arg(r#"{"name":"PolicyRemote","age":41}"#)
            .arg("--json"),
    );
    let denied_main_stderr = String::from_utf8(denied_main_change.stderr).unwrap();
    assert!(
        denied_main_stderr.contains("denied")
            && denied_main_stderr.contains("change")
            && denied_main_stderr.contains("main"),
        "expected change-on-main denial, got: {denied_main_stderr}"
    );

    // bruno can create an unprotected branch.
    let created = parse_stdout_json(&output_success(
        cli()
            .env("OMNIGRAPH_BEARER_TOKEN", "team-token")
            .arg("branch")
            .arg("create")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("--from")
            .arg("main")
            .arg("feature")
            .arg("--json"),
    ));
    assert_eq!(created["name"], "feature");

    // bruno can change the unprotected branch; actor resolves from the token.
    let changed = parse_stdout_json(&output_success(
        cli()
            .env("OMNIGRAPH_BEARER_TOKEN", "team-token")
            .arg("change")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
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
    assert_eq!(changed["actor_id"], "act-bruno");

    // bruno cannot merge into protected main (admins-promote only).
    let denied_merge = output_failure(
        cli()
            .env("OMNIGRAPH_BEARER_TOKEN", "team-token")
            .arg("branch")
            .arg("merge")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("feature")
            .arg("--into")
            .arg("main")
            .arg("--json"),
    );
    let denied_merge_stderr = String::from_utf8(denied_merge.stderr).unwrap();
    assert!(
        denied_merge_stderr.contains("denied") && denied_merge_stderr.contains("branch_merge"),
        "expected branch_merge denial, got: {denied_merge_stderr}"
    );

    // ragnor (admins) can promote into protected main.
    let merged = parse_stdout_json(&output_success(
        cli()
            .env("OMNIGRAPH_BEARER_TOKEN", "admin-token")
            .arg("branch")
            .arg("merge")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("feature")
            .arg("--into")
            .arg("main")
            .arg("--json"),
    ));
    assert_eq!(merged["target"], "main");

    let verify = parse_stdout_json(&output_success(
        cli()
            .env("OMNIGRAPH_BEARER_TOKEN", "team-token")
            .arg("read")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg(GRAPH_ID)
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"PolicyRemote"}"#)
            .arg("--json"),
    ));
    assert_eq!(verify["row_count"], 1);
    assert_eq!(verify["rows"][0]["p.name"], "PolicyRemote");
}

// ─── MR-668 PR 8 — omnigraph graphs list end-to-end ────────────────────────

/// Multi-graph server + CLI `omnigraph graphs list` end-to-end (RFC-011
/// cluster-only serving).
///
/// Steps:
///   1. Build a converged cluster serving one graph `alpha` with a
///      server-scoped policy granting `act-admin` the `graph_list` action.
///   2. Spawn the server with `--cluster` + a bearer-token map.
///   3. `omnigraph graphs list --server <url>` (admin token) — expect `alpha`.
///   4. Addressing the server via `--server <url>` with NO `--graph` errors and
///      lists the candidate graphs (RFC-011 D7).
///
/// Ignored by default — spawning servers needs loopback socket
/// permissions some sandboxes lack.
#[test]
#[ignore = "requires loopback socket permissions in sandboxed runners"]
fn graphs_list_against_multi_graph_server() {
    let cfg_dir = tempfile::tempdir().unwrap();
    let dir = cfg_dir.path();
    fs::copy(fixture("test.pg"), dir.join("alpha.pg")).unwrap();
    fs::write(
        dir.join("server.policy.yaml"),
        GRAPH_LIST_SERVER_POLICY_YAML,
    )
    .unwrap();
    fs::write(
        dir.join("cluster.yaml"),
        "version: 1\nmetadata:\n  name: sys\nstate:\n  backend: cluster\n  lock: true\ngraphs:\n  alpha:\n    schema: ./alpha.pg\npolicies:\n  server:\n    file: ./server.policy.yaml\n    applies_to: [cluster]\n",
    )
    .unwrap();
    output_success(cli().arg("cluster").arg("import").arg("--config").arg(dir));
    output_success(cli().arg("cluster").arg("apply").arg("--config").arg(dir));

    let server = spawn_server_with_cluster_env(
        dir,
        &[(
            "OMNIGRAPH_SERVER_BEARER_TOKENS_JSON",
            r#"{"act-admin":"admin-token"}"#,
        )],
    );

    // `graphs list` lists `alpha`.
    let payload = parse_stdout_json(&output_success(
        cli()
            .env("OMNIGRAPH_BEARER_TOKEN", "admin-token")
            .arg("graphs")
            .arg("list")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--json"),
    ));
    let ids: Vec<&str> = payload["graphs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|g| g["graph_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["alpha"]);

    // RFC-011 D7: addressing the multi-graph server via `--server <url>` with no
    // `--graph` errors and lists the candidate graphs (the resolver probes
    // GET /graphs; the default-env token authorizes it).
    let no_graph = cli()
        .env("OMNIGRAPH_BEARER_TOKEN", "admin-token")
        .arg("query")
        .arg("--server")
        .arg(&server.base_url)
        .arg("-e")
        .arg("query q { match { $p: Person { name: \"x\" } } return { $p.name } }")
        .output()
        .unwrap();
    assert!(
        !no_graph.status.success(),
        "multi-graph server with no --graph must error"
    );
    let stderr = String::from_utf8_lossy(&no_graph.stderr);
    assert!(
        stderr.contains("alpha") && stderr.contains("--graph <id>"),
        "expected a candidate-listing error naming alpha; got: {stderr}"
    );

    drop(server);
}
