mod support;

use std::env;
use std::fs;

use reqwest::blocking::Client;
use serde_json::Value;

use support::*;

const POLICY_E2E_YAML: &str = r#"
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
  - id: admins-write
    allow:
      actors: { group: admins }
      actions: [change]
      branch_scope: any
  - id: admins-branch-ops
    allow:
      actors: { group: admins }
      actions: [branch_create, branch_delete]
      target_branch_scope: any
  - id: admins-schema-apply
    allow:
      actors: { group: admins }
      actions: [schema_apply]
      target_branch_scope: any
"#;

const POLICY_E2E_TESTS_YAML: &str = r#"
version: 1
cases:
  - id: deny-main-change
    actor: act-bruno
    action: change
    branch: main
    expect: deny
  - id: allow-feature-change
    actor: act-bruno
    action: change
    branch: feature
    expect: allow
"#;

fn yaml_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn local_policy_config(graph: &SystemGraph) -> String {
    format!(
        "\
project:
  name: policy-e2e-local
graphs:
  local:
    uri: {}
    policy:
      file: ./policy.yaml
cli:
  graph: local
  branch: main
query:
  roots:
    - .
",
        yaml_string(&graph.path().to_string_lossy())
    )
}

fn local_policy_server_graph_config(graph: &SystemGraph) -> String {
    format!(
        "\
project:
  name: policy-e2e-local
graphs:
  local:
    uri: {}
    policy:
      file: ./policy.yaml
server:
  graph: local
cli:
  branch: main
query:
  roots:
    - .
",
        yaml_string(&graph.path().to_string_lossy())
    )
}

fn insert_person_query(graph: &SystemGraph, name: &str) -> std::path::PathBuf {
    graph.write_query(
        name,
        r#"
query insert_person($name: String, $age: I32) {
    insert Person { name: $name, age: $age }
}
"#,
    )
}

fn add_friend_query(graph: &SystemGraph, name: &str) -> std::path::PathBuf {
    graph.write_query(
        name,
        r#"
query add_friend($from: String, $to: String) {
    insert Knows { from: $from, to: $to }
}
"#,
    )
}

fn snapshot_table_row_count(graph: &SystemGraph, table_key: &str) -> u64 {
    snapshot_table_row_count_at(graph.path(), table_key)
}

fn snapshot_table_row_count_at(graph: &std::path::Path, table_key: &str) -> u64 {
    let payload = parse_stdout_json(&output_success(
        cli().arg("snapshot").arg(graph).arg("--json"),
    ));
    payload["tables"]
        .as_array()
        .unwrap()
        .iter()
        .find(|table| table["table_key"] == table_key)
        .unwrap()["row_count"]
        .as_u64()
        .unwrap()
}

fn gemini_base_url() -> String {
    env::var("OMNIGRAPH_GEMINI_BASE_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "https://generativelanguage.googleapis.com/v1beta".to_string())
}

fn embed_text_with_gemini(text: &str, dim: usize) -> Vec<f32> {
    let api_key = env::var("GEMINI_API_KEY").expect("GEMINI_API_KEY must be set");
    let client = Client::new();
    let response = client
        .post(format!(
            "{}/models/gemini-embedding-2-preview:embedContent",
            gemini_base_url().trim_end_matches('/')
        ))
        .header("x-goog-api-key", api_key)
        .json(&serde_json::json!({
            "model": "models/gemini-embedding-2-preview",
            "content": {
                "parts": [
                    {
                        "text": text
                    }
                ]
            },
            "taskType": "RETRIEVAL_QUERY",
            "outputDimensionality": dim,
        }))
        .send()
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<Value>()
        .unwrap();

    response["embedding"]["values"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_f64().unwrap() as f32)
        .collect()
}

fn format_vector(values: &[f32]) -> String {
    values
        .iter()
        .map(|value| format!("{:.8}", value))
        .collect::<Vec<_>>()
        .join(", ")
}

fn s3_test_graph_uri(suite: &str) -> Option<String> {
    let bucket = env::var("OMNIGRAPH_S3_TEST_BUCKET").ok()?;
    let prefix = env::var("OMNIGRAPH_S3_TEST_PREFIX")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "omnigraph-itests".to_string());
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_nanos();
    Some(format!("s3://{}/{}/{}/{}", bucket, prefix, suite, unique))
}

#[test]
fn local_cli_end_to_end_init_load_read_change_read_flow() {
    let graph = SystemGraph::initialized();
    let mutation_file = insert_person_query(&graph, "system-local-init-change.gq");

    output_success(
        cli()
            .arg("load")
            .arg("--data")
            .arg(fixture("test.jsonl"))
            .arg(graph.path()),
    );

    let read_before = parse_stdout_json(&output_success(
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
    assert_eq!(read_before["row_count"], 1);
    assert_eq!(read_before["rows"][0]["p.name"], "Alice");

    let change_payload = parse_stdout_json(&output_success(
        cli()
            .arg("change")
            .arg(graph.path())
            .arg("--query")
            .arg(&mutation_file)
            .arg("--params")
            .arg(r#"{"name":"Eve","age":29}"#)
            .arg("--json"),
    ));
    assert_eq!(change_payload["branch"], "main");
    assert_eq!(change_payload["affected_nodes"], 1);

    let read_after = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg(graph.path())
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("--name")
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"Eve"}"#)
            .arg("--json"),
    ));
    assert_eq!(read_after["row_count"], 1);
    assert_eq!(read_after["rows"][0]["p.name"], "Eve");

    // Inline-source variants of the same read/change flow (CLI `-e` /
    // `--query-string`). Confirms that file-less invocations reach the
    // engine identically, including param binding and `branch=main` defaults.
    let inline_change = parse_stdout_json(&output_success(
        cli()
            .arg("change")
            .arg(graph.path())
            .arg("-e")
            .arg("query add($name: String, $age: I32) { insert Person { name: $name, age: $age } }")
            .arg("--params")
            .arg(r#"{"name":"Inline","age":42}"#)
            .arg("--json"),
    ));
    assert_eq!(inline_change["branch"], "main");
    assert_eq!(inline_change["query_name"], "add");
    assert_eq!(inline_change["affected_nodes"], 1);

    let inline_read = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg(graph.path())
            .arg("--query-string")
            .arg("query find($name: String) { match { $p: Person { name: $name } } return { $p.name, $p.age } }")
            .arg("--params")
            .arg(r#"{"name":"Inline"}"#)
            .arg("--json"),
    ));
    assert_eq!(inline_read["row_count"], 1);
    assert_eq!(inline_read["rows"][0]["p.name"], "Inline");
    assert_eq!(inline_read["rows"][0]["p.age"], 42);
}

#[test]
fn local_cli_end_to_end_branch_change_merge_flow() {
    let graph = SystemGraph::loaded();
    let mutation_file = insert_person_query(&graph, "system-local-change.gq");

    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--uri")
            .arg(graph.path())
            .arg("--from")
            .arg("main")
            .arg("feature"),
    );

    let change_payload = parse_stdout_json(&output_success(
        cli()
            .arg("change")
            .arg(graph.path())
            .arg("--query")
            .arg(&mutation_file)
            .arg("--branch")
            .arg("feature")
            .arg("--params")
            .arg(r#"{"name":"Zoe","age":33}"#)
            .arg("--json"),
    ));
    assert_eq!(change_payload["branch"], "feature");
    assert_eq!(change_payload["affected_nodes"], 1);

    let feature_read = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg(graph.path())
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("--name")
            .arg("get_person")
            .arg("--branch")
            .arg("feature")
            .arg("--params")
            .arg(r#"{"name":"Zoe"}"#)
            .arg("--json"),
    ));
    assert_eq!(feature_read["row_count"], 1);
    assert_eq!(feature_read["rows"][0]["p.name"], "Zoe");

    let merge_payload = parse_stdout_json(&output_success(
        cli()
            .arg("branch")
            .arg("merge")
            .arg("--uri")
            .arg(graph.path())
            .arg("feature")
            .arg("--json"),
    ));
    assert_eq!(merge_payload["target"], "main");

    let main_read = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg(graph.path())
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("--name")
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"Zoe"}"#)
            .arg("--json"),
    ));
    assert_eq!(main_read["row_count"], 1);
    assert_eq!(main_read["rows"][0]["p.name"], "Zoe");

    // `omnigraph run list` removed. Audit visible via commit list.
    let commits_payload = parse_stdout_json(&output_success(
        cli()
            .arg("commit")
            .arg("list")
            .arg(graph.path())
            .arg("--branch")
            .arg("main")
            .arg("--json"),
    ));
    assert!(commits_payload["commits"].as_array().unwrap().len() >= 2);
}

#[test]
fn local_cli_ingest_creates_review_branch_and_keeps_it_readable() {
    let graph = SystemGraph::loaded();
    let ingest_data = graph.write_jsonl(
        "system-local-ingest.jsonl",
        r#"{"type":"Person","data":{"name":"Zoe","age":33}}
{"type":"Person","data":{"name":"Bob","age":26}}"#,
    );

    let ingest_payload = parse_stdout_json(&output_success(
        cli()
            .arg("ingest")
            .arg("--data")
            .arg(&ingest_data)
            .arg("--branch")
            .arg("feature-ingest")
            .arg(graph.path())
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
            .arg(graph.path())
            .arg("--branch")
            .arg("feature-ingest")
            .arg("--json"),
    ));
    assert_eq!(feature_snapshot["branch"], "feature-ingest");

    let zoe = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg(graph.path())
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

    let bob = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg(graph.path())
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
}

#[test]
fn local_cli_export_round_trips_full_branch_graph() {
    let graph = SystemGraph::loaded();

    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--uri")
            .arg(graph.path())
            .arg("--from")
            .arg("main")
            .arg("feature"),
    );

    let feature_data = graph.write_jsonl(
        "system-local-export-feature.jsonl",
        r#"{"type":"Person","data":{"name":"Eve","age":29}}
{"edge":"Knows","from":"Alice","to":"Eve"}"#,
    );
    output_success(
        cli()
            .arg("load")
            .arg("--data")
            .arg(&feature_data)
            .arg("--branch")
            .arg("feature")
            .arg("--mode")
            .arg("append")
            .arg(graph.path()),
    );

    let exported = stdout_string(&output_success(
        cli()
            .arg("export")
            .arg(graph.path())
            .arg("--branch")
            .arg("feature")
            .arg("--jsonl"),
    ));
    let export_path = graph.write_jsonl("system-local-exported.jsonl", &exported);
    let imported_graph = graph.path().parent().unwrap().join("imported-export.omni");

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
            .arg("--data")
            .arg(&export_path)
            .arg(&imported_graph),
    );

    assert_eq!(
        snapshot_table_row_count_at(&imported_graph, "node:Person"),
        5
    );
    assert_eq!(
        snapshot_table_row_count_at(&imported_graph, "node:Company"),
        2
    );
    assert_eq!(
        snapshot_table_row_count_at(&imported_graph, "edge:Knows"),
        4
    );
    assert_eq!(
        snapshot_table_row_count_at(&imported_graph, "edge:WorksAt"),
        2
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

    let friends = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg(&imported_graph)
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("--name")
            .arg("friends_of")
            .arg("--params")
            .arg(r#"{"name":"Alice"}"#)
            .arg("--json"),
    ));
    assert_eq!(friends["row_count"], 3);
}

#[test]
fn local_cli_s3_end_to_end_init_load_read_flow() {
    let Some(graph_uri) = s3_test_graph_uri("cli-local") else {
        eprintln!("skipping s3 cli test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };

    let temp = tempfile::tempdir().unwrap();
    let query_root = temp.path();
    let config = query_root.join("omnigraph.yaml");
    let query = query_root.join("test.gq");
    fs::copy(fixture("test.gq"), &query).unwrap();
    write_config(
        &config,
        &format!(
            "\
graphs:
  rustfs:
    uri: '{}'
cli:
  graph: rustfs
  branch: main
query:
  roots:
    - .
policy: {{}}
",
            graph_uri
        ),
    );

    output_success(
        cli()
            .arg("init")
            .arg("--schema")
            .arg(fixture("test.pg"))
            .arg(&graph_uri),
    );
    output_success(
        cli()
            .arg("load")
            .arg("--data")
            .arg(fixture("test.jsonl"))
            .arg(&graph_uri),
    );

    let read = parse_stdout_json(&output_success(
        cli()
            .current_dir(query_root)
            .arg("read")
            .arg("--config")
            .arg(&config)
            .arg("--query")
            .arg("test.gq")
            .arg("--name")
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"Alice"}"#)
            .arg("--json"),
    ));
    assert_eq!(read["row_count"], 1);
    assert_eq!(read["rows"][0]["p.name"], "Alice");

    let snapshot = parse_stdout_json(&output_success(
        cli()
            .current_dir(query_root)
            .arg("snapshot")
            .arg("--config")
            .arg(&config)
            .arg("--json"),
    ));
    assert!(snapshot["tables"].is_array());
}

#[test]
fn local_cli_failed_load_keeps_target_state_unchanged() {
    let graph = SystemGraph::loaded();
    let bad_data = graph.write_jsonl(
        "system-bad-load.jsonl",
        r#"{"edge":"Knows","from":"Alice","to":"Missing"}"#,
    );
    let person_rows_before = snapshot_table_row_count(&graph, "node:Person");
    let knows_rows_before = snapshot_table_row_count(&graph, "edge:Knows");

    let output = output_failure(
        cli()
            .arg("load")
            .arg("--data")
            .arg(&bad_data)
            .arg("--mode")
            .arg("append")
            .arg(graph.path()),
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("not found") || stderr.contains("Missing"));

    assert_eq!(
        snapshot_table_row_count(&graph, "node:Person"),
        person_rows_before
    );
    assert_eq!(
        snapshot_table_row_count(&graph, "edge:Knows"),
        knows_rows_before
    );
    // Failed loads leave no run record (the run lifecycle has been
    // removed); atomicity is verified above by the unchanged target.
}

#[test]
fn local_cli_failed_change_keeps_target_state_unchanged() {
    let graph = SystemGraph::loaded();
    let mutation_file = add_friend_query(&graph, "system-invalid-change.gq");

    let output = output_failure(
        cli()
            .arg("change")
            .arg(graph.path())
            .arg("--query")
            .arg(&mutation_file)
            .arg("--params")
            .arg(r#"{"from":"Alice","to":"Missing"}"#),
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("not found") || stderr.contains("Missing"));

    let friends_payload = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg(graph.path())
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("--name")
            .arg("friends_of")
            .arg("--params")
            .arg(r#"{"name":"Alice"}"#)
            .arg("--json"),
    ));
    assert_eq!(friends_payload["row_count"], 2);
    // Failed mutations leave no run record (the run lifecycle has been
    // removed); atomicity is verified above by the unchanged target.
}

#[test]
fn local_cli_resolves_relative_query_against_config_base_dir() {
    let graph = SystemGraph::loaded();
    let root = graph.path().parent().unwrap();
    let config_dir = root.join("config");
    let query_dir = config_dir.join("queries");
    let ambient_dir = root.join("ambient");
    fs::create_dir_all(&query_dir).unwrap();
    fs::create_dir_all(&ambient_dir).unwrap();

    let config = config_dir.join("omnigraph.yaml");
    write_config(
        &config,
        &format!(
            "\
graphs:
  local:
    uri: '{}'
cli:
  graph: local
  branch: main
query:
  roots:
    - queries
policy: {{}}
",
            graph.path().display()
        ),
    );
    write_query_file(
        &query_dir.join("local.gq"),
        r#"
query get_person($name: String) {
    match {
        $p: Person { name: $name }
    }
    return { $p.age, $p.name }
}
"#,
    );
    write_query_file(
        &ambient_dir.join("local.gq"),
        r#"
query get_person($name: String) {
    match {
        $p: Person { name: $name }
    }
    return { $p.name }
}
"#,
    );

    let payload = parse_stdout_json(&output_success(
        cli()
            .current_dir(&ambient_dir)
            .arg("read")
            .arg("--config")
            .arg(&config)
            .arg("--query")
            .arg("local.gq")
            .arg("--name")
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"Alice"}"#)
            .arg("--json"),
    ));
    let columns = payload["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(columns, vec!["p.age", "p.name"]);
    assert_eq!(payload["rows"][0]["p.age"], 30);
    assert_eq!(payload["rows"][0]["p.name"], "Alice");
}

#[test]
fn local_cli_datetime_and_list_types_round_trip_through_load_read_and_change() {
    let temp = tempfile::tempdir().unwrap();
    let graph = graph_path(temp.path());
    let schema = temp.path().join("datatypes.pg");
    let data = temp.path().join("datatypes.jsonl");
    let queries = temp.path().join("datatypes.gq");

    write_query_file(
        &schema,
        r#"
node Task {
    slug: String @key
    title: String
    due_at: DateTime
    tags: [String]
    scores: [I32]?
    active_days: [Date]?
}
"#,
    );
    write_jsonl(
        &data,
        r#"{"type":"Task","data":{"slug":"alpha","title":"Launch prep","due_at":"2026-04-01T08:30:00Z","tags":["launch","priority"],"scores":[1,2],"active_days":["2026-03-30","2026-03-31"]}}
{"type":"Task","data":{"slug":"beta","title":"Archive","due_at":"2026-05-01T12:00:00Z","tags":["backlog"],"scores":[5],"active_days":["2026-04-01"]}}"#,
    );
    write_query_file(
        &queries,
        r#"
query due_with_tag($deadline: DateTime, $tag: String) {
    match {
        $t: Task
        $t.due_at <= $deadline
        $t.tags contains $tag
    }
    return { $t.slug, $t.due_at, $t.tags, $t.scores, $t.active_days }
}

query insert_task(
    $slug: String,
    $title: String,
    $due_at: DateTime,
    $tags: [String],
    $scores: [I32],
    $active_days: [Date]
) {
    insert Task {
        slug: $slug,
        title: $title,
        due_at: $due_at,
        tags: $tags,
        scores: $scores,
        active_days: $active_days
    }
}

query update_task(
    $slug: String,
    $due_at: DateTime,
    $tags: [String],
    $scores: [I32],
    $active_days: [Date]
) {
    update Task set {
        due_at: $due_at,
        tags: $tags,
        scores: $scores,
        active_days: $active_days
    } where slug = $slug
}

query get_task($slug: String) {
    match { $t: Task { slug: $slug } }
    return { $t.slug, $t.due_at, $t.tags, $t.scores, $t.active_days }
}
"#,
    );

    output_success(cli().arg("init").arg("--schema").arg(&schema).arg(&graph));
    output_success(cli().arg("load").arg("--data").arg(&data).arg(&graph));

    let filtered = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg(&graph)
            .arg("--query")
            .arg(&queries)
            .arg("--name")
            .arg("due_with_tag")
            .arg("--params")
            .arg(r#"{"deadline":"2026-04-02T00:00:00Z","tag":"launch"}"#)
            .arg("--json"),
    ));
    assert_eq!(filtered["row_count"], 1);
    assert_eq!(filtered["rows"][0]["t.slug"], "alpha");
    assert_eq!(filtered["rows"][0]["t.due_at"], "2026-04-01T08:30:00.000Z");
    assert_eq!(
        filtered["rows"][0]["t.tags"],
        serde_json::json!(["launch", "priority"])
    );
    assert_eq!(filtered["rows"][0]["t.scores"], serde_json::json!([1, 2]));
    assert_eq!(
        filtered["rows"][0]["t.active_days"],
        serde_json::json!(["2026-03-30", "2026-03-31"])
    );

    let insert_payload = parse_stdout_json(&output_success(
        cli()
            .arg("change")
            .arg(&graph)
            .arg("--query")
            .arg(&queries)
            .arg("--name")
            .arg("insert_task")
            .arg("--params")
            .arg(
                r#"{"slug":"gamma","title":"Embed prep","due_at":"2026-04-03T09:15:00Z","tags":["embed","launch"],"scores":[3,8],"active_days":["2026-04-02","2026-04-03"]}"#,
            )
            .arg("--json"),
    ));
    assert_eq!(insert_payload["affected_nodes"], 1);

    let update_payload = parse_stdout_json(&output_success(
        cli()
            .arg("change")
            .arg(&graph)
            .arg("--query")
            .arg(&queries)
            .arg("--name")
            .arg("update_task")
            .arg("--params")
            .arg(r#"{"slug":"gamma","due_at":"2026-04-04T10:45:00Z","tags":["embed","released"],"scores":[13,21],"active_days":["2026-04-04","2026-04-05"]}"#)
            .arg("--json"),
    ));
    assert_eq!(update_payload["affected_nodes"], 1);

    let gamma = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg(&graph)
            .arg("--query")
            .arg(&queries)
            .arg("--name")
            .arg("get_task")
            .arg("--params")
            .arg(r#"{"slug":"gamma"}"#)
            .arg("--json"),
    ));
    assert_eq!(gamma["row_count"], 1);
    assert_eq!(gamma["rows"][0]["t.slug"], "gamma");
    assert_eq!(gamma["rows"][0]["t.due_at"], "2026-04-04T10:45:00.000Z");
    assert_eq!(
        gamma["rows"][0]["t.tags"],
        serde_json::json!(["embed", "released"])
    );
    assert_eq!(gamma["rows"][0]["t.scores"], serde_json::json!([13, 21]));
    assert_eq!(
        gamma["rows"][0]["t.active_days"],
        serde_json::json!(["2026-04-04", "2026-04-05"])
    );
}

#[test]
#[ignore = "requires GEMINI_API_KEY and network access"]
fn local_cli_real_gemini_string_nearest_query_returns_expected_match() {
    let temp = tempfile::tempdir().unwrap();
    let graph = graph_path(temp.path());
    let schema = temp.path().join("gemini.pg");
    let data = temp.path().join("gemini.jsonl");
    let queries = temp.path().join("gemini.gq");

    write_query_file(
        &schema,
        r#"
node Doc {
    slug: String @key
    title: String
    embedding: Vector(4) @index
}
"#,
    );

    let alpha = embed_text_with_gemini("alpha", 4);
    let beta = embed_text_with_gemini("beta", 4);
    let gamma = embed_text_with_gemini("gamma", 4);
    write_jsonl(
        &data,
        &format!(
            r#"{{"type":"Doc","data":{{"slug":"alpha-doc","title":"alpha","embedding":[{}]}}}}
{{"type":"Doc","data":{{"slug":"beta-doc","title":"beta","embedding":[{}]}}}}
{{"type":"Doc","data":{{"slug":"gamma-doc","title":"gamma","embedding":[{}]}}}}"#,
            format_vector(&alpha),
            format_vector(&beta),
            format_vector(&gamma),
        ),
    );
    write_query_file(
        &queries,
        r#"
query vector_search($q: String) {
    match { $d: Doc }
    return { $d.slug, $d.title }
    order { nearest($d.embedding, $q) }
    limit 3
}
"#,
    );

    output_success(cli().arg("init").arg("--schema").arg(&schema).arg(&graph));
    output_success(cli().arg("load").arg("--data").arg(&data).arg(&graph));

    let result = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg(&graph)
            .arg("--query")
            .arg(&queries)
            .arg("--name")
            .arg("vector_search")
            .arg("--params")
            .arg(r#"{"q":"alpha"}"#)
            .arg("--json"),
    ));

    assert_eq!(result["row_count"], 3);
    assert_eq!(result["rows"][0]["d.slug"], "alpha-doc");
}

// The publisher CAS conflict shape is verified end-to-end at the engine
// level in
// `crates/omnigraph/tests/writes.rs::concurrent_writers_one_succeeds_one_gets_expected_version_mismatch`
// and at the HTTP boundary in
// `crates/omnigraph-server/tests/server.rs::change_conflict_returns_manifest_conflict_409`.
// A CLI-level race would be timing-dependent; with direct-publish the
// surface is the same engine path the unit test already covers.

#[test]
fn local_cli_policy_tooling_is_end_to_end() {
    // Sanity check for the read-only policy CLI surfaces. These don't
    // mutate the graph; they parse and evaluate the effective policy for
    // named graph selections, including per-graph policy files.
    let graph = SystemGraph::loaded();
    let config = graph.write_config("omnigraph-policy.yaml", &local_policy_config(&graph));
    let server_graph_config = graph.write_config(
        "omnigraph-policy-server.yaml",
        &local_policy_server_graph_config(&graph),
    );
    graph.write_config("policy.yaml", POLICY_E2E_YAML);
    graph.write_config("policy.tests.yaml", POLICY_E2E_TESTS_YAML);

    for config in [&config, &server_graph_config] {
        let validate = output_success(
            cli()
                .arg("policy")
                .arg("validate")
                .arg("--config")
                .arg(config),
        );
        assert!(stdout_string(&validate).contains("policy valid:"));

        let tests = output_success(cli().arg("policy").arg("test").arg("--config").arg(config));
        assert!(stdout_string(&tests).contains("policy tests passed: 2 cases"));

        let explain = output_success(
            cli()
                .arg("policy")
                .arg("explain")
                .arg("--config")
                .arg(config)
                .arg("--actor")
                .arg("act-bruno")
                .arg("--action")
                .arg("change")
                .arg("--branch")
                .arg("main"),
        );
        let explain_stdout = stdout_string(&explain);
        assert!(explain_stdout.contains("decision: deny"));
        assert!(explain_stdout.contains("branch: main"));
    }
}

#[test]
fn local_cli_change_enforces_engine_layer_policy() {
    // Asserts MR-722 PR #4: when the selected graph has a configured
    // policy file, the CLI loads PolicyEngine into Omnigraph and every
    // direct-engine write hits `enforce(action, scope, actor)` — identical
    // to what the HTTP server gets, regardless of transport.
    //
    // Three cases, each discriminating:
    //
    // 1. Policy installed, no actor source (no `cli.actor` in config,
    //    no `--as` flag) → engine-layer footgun guard fires; CLI exits
    //    non-zero with a "no actor" message. Silent bypass is the bug
    //    PR #4 prevents.
    // 2. Policy installed, `--as act-bruno`, change on main → Cedar
    //    denies (bruno can change unprotected branches; main is
    //    protected). CLI exits non-zero with a "denied" message.
    // 3. Policy installed, `--as act-ragnor`, change on main →
    //    Cedar permits (admins-write rule). Write succeeds and the
    //    inserted row is readable.
    let graph = SystemGraph::loaded();
    let config = graph.write_config("omnigraph-policy.yaml", &local_policy_config(&graph));
    graph.write_config("policy.yaml", POLICY_E2E_YAML);
    let mutation_file = insert_person_query(&graph, "system-local-policy-change.gq");

    // Case 1: policy configured, no actor threaded → footgun guard.
    let no_actor = output_failure(
        cli()
            .arg("change")
            .arg("--config")
            .arg(&config)
            .arg("--query")
            .arg(&mutation_file)
            .arg("--params")
            .arg(r#"{"name":"NoActorPerson","age":1}"#)
            .arg("--json"),
    );
    let no_actor_stderr = String::from_utf8_lossy(&no_actor.stderr);
    assert!(
        no_actor_stderr.contains("no actor"),
        "expected 'no actor' footgun message, got stderr: {no_actor_stderr}"
    );

    // Case 2: `--as act-bruno` against protected main → denied.
    let denied = output_failure(
        cli()
            .arg("--as")
            .arg("act-bruno")
            .arg("change")
            .arg("--config")
            .arg(&config)
            .arg("--query")
            .arg(&mutation_file)
            .arg("--params")
            .arg(r#"{"name":"BrunoOnMain","age":2}"#)
            .arg("--json"),
    );
    let denied_stderr = String::from_utf8_lossy(&denied.stderr);
    assert!(
        denied_stderr.contains("denied"),
        "expected 'denied' message for bruno/main, got stderr: {denied_stderr}"
    );

    // Case 3: `--as act-ragnor` against main → permitted by admins-write.
    let allowed = parse_stdout_json(&output_success(
        cli()
            .arg("--as")
            .arg("act-ragnor")
            .arg("change")
            .arg("--config")
            .arg(&config)
            .arg("--query")
            .arg(&mutation_file)
            .arg("--params")
            .arg(r#"{"name":"RagnorOnMain","age":3}"#)
            .arg("--json"),
    ));
    assert_eq!(allowed["branch"], "main");
    assert_eq!(allowed["affected_nodes"], 1);
    assert_eq!(allowed["actor_id"], "act-ragnor");

    // Verify the row landed — proves the write actually committed, not
    // just that enforce returned Ok and silently dropped the work.
    let verify = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg(graph.path())
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("--name")
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"RagnorOnMain"}"#)
            .arg("--json"),
    ));
    assert_eq!(verify["row_count"], 1);
    assert_eq!(verify["rows"][0]["p.name"], "RagnorOnMain");
}

#[test]
fn local_cli_positional_uri_does_not_inherit_default_graph_policy() {
    let graph = SystemGraph::loaded();
    let config = graph.write_config("omnigraph-policy.yaml", &local_policy_config(&graph));
    graph.write_config("policy.yaml", POLICY_E2E_YAML);
    let mutation_file = insert_person_query(&graph, "system-local-policy-positional.gq");

    let allowed = parse_stdout_json(&output_success(
        cli()
            .arg("--as")
            .arg("act-bruno")
            .arg("change")
            .arg("--config")
            .arg(&config)
            .arg("--uri")
            .arg(graph.path())
            .arg("--query")
            .arg(&mutation_file)
            .arg("--params")
            .arg(r#"{"name":"PositionalUriBruno","age":4}"#)
            .arg("--json"),
    ));
    assert_eq!(allowed["affected_nodes"], 1);
    assert_eq!(allowed["actor_id"], "act-bruno");
}

// ─── MR-722 PR A: CLI×writer matrix ───────────────────────────────────────
//
// The change writer is covered above by `local_cli_change_enforces_engine_layer_policy`.
// These tests extend the engine-layer-policy assertion to the other 6
// writers, asserting each `omnigraph <writer> --as <actor>` invocation
// reaches the corresponding `_as` method and Cedar evaluates correctly.
// One denied case (`--as act-bruno`) + one allowed case (`--as act-ragnor`
// via the `admins-*` rules) per writer; the no-actor footgun is already
// proved by the change-writer test and applies identically to every
// other `_as` variant.

#[test]
fn local_cli_load_enforces_engine_layer_policy() {
    let graph = SystemGraph::loaded();
    let config = graph.write_config("omnigraph-policy.yaml", &local_policy_config(&graph));
    graph.write_config("policy.yaml", POLICY_E2E_YAML);
    let data = graph.write_jsonl(
        "system-local-policy-load.jsonl",
        r#"{"type":"Person","data":{"name":"LoadPolicy","age":11}}"#,
    );

    // act-bruno: change-on-protected is denied (team-write-unprotected only).
    let denied = output_failure(
        cli()
            .arg("--as")
            .arg("act-bruno")
            .arg("load")
            .arg("--config")
            .arg(&config)
            .arg("--data")
            .arg(&data)
            .arg("--json"),
    );
    let stderr = String::from_utf8_lossy(&denied.stderr);
    assert!(
        stderr.contains("denied"),
        "expected 'denied' for bruno/main load, got: {stderr}"
    );

    // act-ragnor: admins-write rule permits change anywhere.
    let allowed = parse_stdout_json(&output_success(
        cli()
            .arg("--as")
            .arg("act-ragnor")
            .arg("load")
            .arg("--config")
            .arg(&config)
            .arg("--data")
            .arg(&data)
            .arg("--json"),
    ));
    assert_eq!(allowed["branch"], "main");
    assert!(allowed["nodes_loaded"].as_u64().unwrap() >= 1);
}

#[test]
fn local_cli_ingest_enforces_engine_layer_policy() {
    let graph = SystemGraph::loaded();
    let config = graph.write_config("omnigraph-policy.yaml", &local_policy_config(&graph));
    graph.write_config("policy.yaml", POLICY_E2E_YAML);
    let data = graph.write_jsonl(
        "system-local-policy-ingest.jsonl",
        r#"{"type":"Person","data":{"name":"IngestPolicy","age":12}}"#,
    );

    // act-bruno: ingest into a new branch requires both BranchCreate and
    // Change. Bruno has change-unprotected only, and the implicit
    // branch_create fires first when the target branch doesn't exist.
    // Either gate is enough to deny — assert denial without pinning
    // which one fires first.
    let denied = output_failure(
        cli()
            .arg("--as")
            .arg("act-bruno")
            .arg("ingest")
            .arg("--config")
            .arg(&config)
            .arg("--data")
            .arg(&data)
            .arg("--branch")
            .arg("policy-ingest-feature")
            .arg("--json"),
    );
    let stderr = String::from_utf8_lossy(&denied.stderr);
    assert!(
        stderr.contains("denied"),
        "expected 'denied' for bruno ingest, got: {stderr}"
    );

    // act-ragnor: admins-write covers Change, admins-branch-ops covers
    // BranchCreate. Both fire as ingest creates the branch + loads.
    let allowed = parse_stdout_json(&output_success(
        cli()
            .arg("--as")
            .arg("act-ragnor")
            .arg("ingest")
            .arg("--config")
            .arg(&config)
            .arg("--data")
            .arg(&data)
            .arg("--branch")
            .arg("policy-ingest-feature")
            .arg("--json"),
    ));
    assert_eq!(allowed["branch"], "policy-ingest-feature");
    assert_eq!(allowed["branch_created"], true);
}

#[test]
fn local_cli_schema_apply_enforces_engine_layer_policy() {
    let graph = SystemGraph::loaded();
    let config = graph.write_config("omnigraph-policy.yaml", &local_policy_config(&graph));
    graph.write_config("policy.yaml", POLICY_E2E_YAML);

    // Additive: add a nullable property; SDK-compatible with the fixture
    // schema. Uses the schema-apply scope (TargetBranch("main")).
    let new_schema = std::fs::read_to_string(fixture("test.pg"))
        .unwrap()
        .replace(
            "    age: I32?\n}",
            "    age: I32?\n    nickname: String?\n}",
        );
    let schema_path = graph.path().join("policy-additive.pg");
    std::fs::write(&schema_path, &new_schema).unwrap();

    let denied = output_failure(
        cli()
            .arg("--as")
            .arg("act-bruno")
            .arg("schema")
            .arg("apply")
            .arg("--config")
            .arg(&config)
            .arg("--schema")
            .arg(&schema_path)
            .arg("--json"),
    );
    let stderr = String::from_utf8_lossy(&denied.stderr);
    assert!(
        stderr.contains("denied"),
        "expected 'denied' for bruno schema apply, got: {stderr}"
    );

    let allowed = parse_stdout_json(&output_success(
        cli()
            .arg("--as")
            .arg("act-ragnor")
            .arg("schema")
            .arg("apply")
            .arg("--config")
            .arg(&config)
            .arg("--schema")
            .arg(&schema_path)
            .arg("--json"),
    ));
    assert_eq!(allowed["applied"], true);
}

#[test]
fn local_cli_schema_apply_rejects_stored_query_breakage_before_publish() {
    let graph = SystemGraph::loaded();
    graph.write_query(
        "stored-find-person.gq",
        "query find_person($name: String) { match { $p: Person { name: $name } } return { $p.age } }",
    );
    let config = graph.write_config(
        "omnigraph-stored-query-schema.yaml",
        &format!(
            "\
graphs:
  local:
    uri: {}
    queries:
      find_person:
        file: ./stored-find-person.gq
cli:
  graph: local
  branch: main
query:
  roots:
    - .
policy: {{}}
",
            yaml_string(&graph.path().to_string_lossy())
        ),
    );
    let renamed_schema = std::fs::read_to_string(fixture("test.pg"))
        .unwrap()
        .replace("age: I32?", "years: I32? @rename_from(\"age\")");
    let schema_path = graph.write_file("stored-query-breaks.pg", &renamed_schema);

    let rejected = output_failure(
        cli()
            .arg("schema")
            .arg("apply")
            .arg("--config")
            .arg(&config)
            .arg("--schema")
            .arg(&schema_path)
            .arg("--json"),
    );
    let stderr = String::from_utf8_lossy(&rejected.stderr);
    assert!(
        stderr.contains("find_person") && stderr.contains("schema check"),
        "schema apply should reject the stored-query breakage before publish; stderr: {stderr}"
    );

    let schema = stdout_string(&output_success(
        cli().arg("schema").arg("show").arg("--config").arg(&config),
    ));
    assert!(schema.contains("age: I32?"));
    assert!(!schema.contains("years: I32?"));
}

#[test]
fn local_cli_branch_create_enforces_engine_layer_policy() {
    let graph = SystemGraph::loaded();
    let config = graph.write_config("omnigraph-policy.yaml", &local_policy_config(&graph));
    graph.write_config("policy.yaml", POLICY_E2E_YAML);

    let denied = output_failure(
        cli()
            .arg("--as")
            .arg("act-bruno")
            .arg("branch")
            .arg("create")
            .arg("--config")
            .arg(&config)
            .arg("--from")
            .arg("main")
            .arg("bruno-feature"),
    );
    let stderr = String::from_utf8_lossy(&denied.stderr);
    assert!(
        stderr.contains("denied"),
        "expected 'denied' for bruno branch create, got: {stderr}"
    );

    output_success(
        cli()
            .arg("--as")
            .arg("act-ragnor")
            .arg("branch")
            .arg("create")
            .arg("--config")
            .arg(&config)
            .arg("--from")
            .arg("main")
            .arg("ragnor-feature"),
    );
}

#[test]
fn local_cli_branch_delete_enforces_engine_layer_policy() {
    let graph = SystemGraph::loaded();
    let config = graph.write_config("omnigraph-policy.yaml", &local_policy_config(&graph));
    graph.write_config("policy.yaml", POLICY_E2E_YAML);

    // Pre-create the branch as ragnor so there's something to delete.
    output_success(
        cli()
            .arg("--as")
            .arg("act-ragnor")
            .arg("branch")
            .arg("create")
            .arg("--config")
            .arg(&config)
            .arg("--from")
            .arg("main")
            .arg("doomed"),
    );

    let denied = output_failure(
        cli()
            .arg("--as")
            .arg("act-bruno")
            .arg("branch")
            .arg("delete")
            .arg("--config")
            .arg(&config)
            .arg("doomed"),
    );
    let stderr = String::from_utf8_lossy(&denied.stderr);
    assert!(
        stderr.contains("denied"),
        "expected 'denied' for bruno branch delete, got: {stderr}"
    );

    output_success(
        cli()
            .arg("--as")
            .arg("act-ragnor")
            .arg("branch")
            .arg("delete")
            .arg("--config")
            .arg(&config)
            .arg("doomed"),
    );
}

#[test]
fn local_cli_branch_merge_enforces_engine_layer_policy() {
    let graph = SystemGraph::loaded();
    let config = graph.write_config("omnigraph-policy.yaml", &local_policy_config(&graph));
    graph.write_config("policy.yaml", POLICY_E2E_YAML);

    // Pre-create a feature branch as ragnor (admins-branch-ops covers it).
    output_success(
        cli()
            .arg("--as")
            .arg("act-ragnor")
            .arg("branch")
            .arg("create")
            .arg("--config")
            .arg(&config)
            .arg("--from")
            .arg("main")
            .arg("merge-feature"),
    );

    let denied = output_failure(
        cli()
            .arg("--as")
            .arg("act-bruno")
            .arg("branch")
            .arg("merge")
            .arg("--config")
            .arg(&config)
            .arg("merge-feature")
            .arg("--into")
            .arg("main"),
    );
    let stderr = String::from_utf8_lossy(&denied.stderr);
    assert!(
        stderr.contains("denied"),
        "expected 'denied' for bruno branch merge, got: {stderr}"
    );

    output_success(
        cli()
            .arg("--as")
            .arg("act-ragnor")
            .arg("branch")
            .arg("merge")
            .arg("--config")
            .arg(&config)
            .arg("merge-feature")
            .arg("--into")
            .arg("main"),
    );
}

// ─── MR-722 PR A: cli.actor config-only precedence ────────────────────────
//
// The change-writer test above uses `--as` directly. These two tests
// pin the precedence rule that `main.rs::resolve_cli_actor` implements:
// `--as` flag > `cli.actor` from `omnigraph.yaml` > None.

fn local_policy_config_with_actor(graph: &SystemGraph, actor: &str) -> String {
    // Mirrors `local_policy_config` but adds `cli.actor` so the
    // config-only precedence path is exercised. The `cli:` block
    // already has `graph` and `branch`; appending `actor` here.
    format!(
        "\
project:
  name: policy-e2e-local
graphs:
  local:
    uri: {}
    policy:
      file: ./policy.yaml
cli:
  graph: local
  branch: main
  actor: {}
query:
  roots:
    - .
",
        yaml_string(&graph.path().to_string_lossy()),
        actor,
    )
}

#[test]
fn local_cli_actor_from_config_used_when_no_flag() {
    // cli.actor: act-ragnor in omnigraph.yaml, no --as flag → change
    // permitted via admins-write rule. Proves the config-only path
    // works; previously the only proof was structural.
    let graph = SystemGraph::loaded();
    let config = graph.write_config(
        "omnigraph-policy.yaml",
        &local_policy_config_with_actor(&graph, "act-ragnor"),
    );
    graph.write_config("policy.yaml", POLICY_E2E_YAML);
    let mutation_file = insert_person_query(&graph, "system-local-cli-actor.gq");

    let allowed = parse_stdout_json(&output_success(
        cli()
            .arg("change")
            .arg("--config")
            .arg(&config)
            .arg("--query")
            .arg(&mutation_file)
            .arg("--params")
            .arg(r#"{"name":"ConfigActorEve","age":18}"#)
            .arg("--json"),
    ));
    assert_eq!(allowed["affected_nodes"], 1);
    assert_eq!(allowed["actor_id"], "act-ragnor");
}

#[test]
fn local_cli_actor_flag_overrides_config_actor() {
    // cli.actor: act-ragnor in config + --as act-bruno on CLI → change
    // denied. Flag wins per the precedence rule. Without this test, a
    // future change that reverses precedence would ride through silently.
    let graph = SystemGraph::loaded();
    let config = graph.write_config(
        "omnigraph-policy.yaml",
        &local_policy_config_with_actor(&graph, "act-ragnor"),
    );
    graph.write_config("policy.yaml", POLICY_E2E_YAML);
    let mutation_file = insert_person_query(&graph, "system-local-cli-actor-override.gq");

    let denied = output_failure(
        cli()
            .arg("--as")
            .arg("act-bruno")
            .arg("change")
            .arg("--config")
            .arg(&config)
            .arg("--query")
            .arg(&mutation_file)
            .arg("--params")
            .arg(r#"{"name":"OverrideEve","age":19}"#)
            .arg("--json"),
    );
    let stderr = String::from_utf8_lossy(&denied.stderr);
    assert!(
        stderr.contains("denied"),
        "expected 'denied' when --as overrides config to bruno, got: {stderr}"
    );
}

/// Phase 5 (RFC-005): "applied means serving" — converge a cluster with the
/// CLI, boot the real omnigraph-server binary with --cluster, and serve the
/// applied stored query over HTTP with zero omnigraph.yaml involvement.
#[test]
fn local_cluster_apply_then_server_boots_from_cluster_state() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(
        temp.path().join("people.pg"),
        "\nnode Person {\n  name: String @key\n}\n",
    )
    .unwrap();
    std::fs::write(
        temp.path().join("people.gq"),
        "\nquery find_person($name: String) {\n  match { $p: Person { name: $name } }\n  return { $p.name }\n}\n",
    )
    .unwrap();
    std::fs::write(
        temp.path().join("cluster.yaml"),
        r#"
version: 1
graphs:
  knowledge:
    schema: ./people.pg
    queries:
      find_person:
        file: ./people.gq
"#,
    )
    .unwrap();
    for command in ["import", "apply"] {
        let output = cli()
            .arg("cluster")
            .arg(command)
            .arg("--config")
            .arg(temp.path())
            .arg("--json")
            .output()
            .unwrap();
        assert!(output.status.success(), "cluster {command} failed");
    }
    // Seed a row through the graph plane so the stored query has data.
    let data = temp.path().join("seed.jsonl");
    std::fs::write(&data, "{\"type\":\"Person\",\"data\":{\"name\":\"Ada\"}}\n").unwrap();
    let output = cli()
        .arg("load")
        .arg("--data")
        .arg(&data)
        .arg(temp.path().join("graphs/knowledge.omni"))
        .output()
        .unwrap();
    assert!(output.status.success(), "graph load failed");

    let server = spawn_server_with_cluster(temp.path());
    let client = reqwest::blocking::Client::new();
    let queries: serde_json::Value = client
        .get(format!("{}/graphs/knowledge/queries", server.base_url))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert!(
        queries["queries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|q| q["name"] == "find_person"),
        "{queries}"
    );
    let response = client
        .post(format!(
            "{}/graphs/knowledge/queries/find_person",
            server.base_url
        ))
        .json(&serde_json::json!({"params": {"name": "Ada"}}))
        .send()
        .unwrap();
    assert!(response.status().is_success(), "{:?}", response.status());
    let body: serde_json::Value = response.json().unwrap();
    assert!(body.to_string().contains("Ada"), "{body}");
}

// ---- Comprehensive full-cycle cluster e2e (Phases 1-5 composed) ----

/// Run a `cluster` subcommand and return its JSON output. Deliberately does
/// NOT assert a zero exit code: blocked/unconverged runs (e.g. an `apply`
/// awaiting an approval) exit non-zero by contract while still emitting the
/// structured output the caller asserts on (`ok`/`converged`/dispositions).
/// Commands where failure is never expected must assert on those fields
/// (every call here checks `ok` or `converged`) or use `cli()` directly with
/// `status.success()`.
fn cluster_cli(dir: &std::path::Path, args: &[&str]) -> serde_json::Value {
    let mut command = cli();
    command.arg("cluster");
    for arg in args {
        command.arg(arg);
    }
    let output = command
        .arg("--config")
        .arg(dir)
        .arg("--json")
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(stdout.trim()).unwrap_or_else(|err| {
        panic!(
            "cluster {args:?} produced unparseable output ({err}): stdout={stdout} stderr={}",
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

fn write_two_graph_cluster(dir: &std::path::Path) {
    std::fs::write(
        dir.join("people.pg"),
        "\nnode Person {\n  name: String @key\n}\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("services.pg"),
        "\nnode Service {\n  name: String @key\n}\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("people.gq"),
        "\nquery find_person($name: String) {\n  match { $p: Person { name: $name } }\n  return { $p.name }\n}\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("services.gq"),
        "\nquery find_service($name: String) {\n  match { $s: Service { name: $name } }\n  return { $s.name }\n}\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("cluster.yaml"),
        r#"
version: 1
graphs:
  knowledge:
    schema: ./people.pg
    queries:
      find_person:
        file: ./people.gq
  engineering:
    schema: ./services.pg
    queries:
      find_service:
        file: ./services.gq
"#,
    )
    .unwrap();
}

fn seed_graph(dir: &std::path::Path, graph: &str, row: &str) {
    let data = dir.join(format!("{graph}-seed.jsonl"));
    std::fs::write(&data, row).unwrap();
    let output = cli()
        .arg("load")
        .arg("--data")
        .arg(&data)
        .arg(dir.join(format!("graphs/{graph}.omni")))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "seed {graph} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn invoke_query(
    client: &Client,
    base_url: &str,
    graph: &str,
    query: &str,
    params: serde_json::Value,
) -> (u16, serde_json::Value) {
    let response = client
        .post(format!("{base_url}/graphs/{graph}/queries/{query}"))
        .json(&serde_json::json!({ "params": params }))
        .send()
        .unwrap();
    let status = response.status().as_u16();
    let body = response.json().unwrap_or(serde_json::Value::Null);
    (status, body)
}

/// Opt-out for the comprehensive system e2es below. They need no external
/// services — only the workspace-built `omnigraph`/`omnigraph-server`
/// binaries (cargo provides them via `CARGO_BIN_EXE_*`), ephemeral localhost
/// ports, and local-FS temp dirs — but they spawn real server processes and
/// run multi-stage lifecycles, so constrained sandboxes can suppress them:
/// `OMNIGRAPH_SKIP_SYSTEM_E2E=1 cargo test ...` (same skip-with-message
/// pattern as the S3 tests' `OMNIGRAPH_S3_TEST_BUCKET` gate).
fn skip_system_e2e(test_name: &str) -> bool {
    if std::env::var("OMNIGRAPH_SKIP_SYSTEM_E2E").is_ok_and(|v| !v.is_empty() && v != "0") {
        eprintln!("skipping {test_name}: OMNIGRAPH_SKIP_SYSTEM_E2E is set");
        return true;
    }
    false
}

/// The whole control-plane story in one test: declare two graphs → converge
/// (apply creates them) → serve → evolve schema+query in one apply → restart
/// serves the new shape → out-of-band drift converged back → approved graph
/// delete → restart serves the survivor only → plan empty.
#[test]
fn local_cluster_full_lifecycle_declare_serve_evolve_delete() {
    if skip_system_e2e("local_cluster_full_lifecycle_declare_serve_evolve_delete") {
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path();
    write_two_graph_cluster(dir);

    // Phase 1-2: declare + record.
    assert_eq!(cluster_cli(dir, &["import"])["ok"], true);
    // Phase 3-4: one apply creates both graphs and publishes the catalog.
    let converge = cluster_cli(dir, &["apply"]);
    assert_eq!(converge["converged"], true, "{converge}");
    seed_graph(dir, "knowledge", "{\"type\":\"Person\",\"data\":{\"name\":\"Ada\"}}\n");
    seed_graph(dir, "engineering", "{\"type\":\"Service\",\"data\":{\"name\":\"billing\"}}\n");

    // Phase 5: serve the applied revision.
    let client = Client::new();
    {
        let server = spawn_server_with_cluster(dir);
        let (status, body) = invoke_query(
            &client,
            &server.base_url,
            "knowledge",
            "find_person",
            serde_json::json!({"name": "Ada"}),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["rows"][0]["p.name"], "Ada", "{body}");
        let (status, body) = invoke_query(
            &client,
            &server.base_url,
            "engineering",
            "find_service",
            serde_json::json!({"name": "billing"}),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["rows"][0]["s.name"], "billing", "{body}");
    }

    // Evolve: schema gains a field, the query returns it — one apply, with
    // the migration previewed in plan.
    std::fs::write(
        dir.join("people.pg"),
        "\nnode Person {\n  name: String @key\n  bio: String?\n}\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("people.gq"),
        "\nquery find_person($name: String) {\n  match { $p: Person { name: $name } }\n  return { $p.name, $p.bio }\n}\n",
    )
    .unwrap();
    let plan = cluster_cli(dir, &["plan"]);
    let schema_change = plan["changes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|change| change["resource"] == "schema.knowledge")
        .unwrap();
    assert_eq!(schema_change["migration"]["supported"], true, "{plan}");
    let evolve = cluster_cli(dir, &["apply"]);
    assert_eq!(evolve["converged"], true, "{evolve}");

    // Restart: the server serves the evolved shape.
    {
        let server = spawn_server_with_cluster(dir);
        let (status, body) = invoke_query(
            &client,
            &server.base_url,
            "knowledge",
            "find_person",
            serde_json::json!({"name": "Ada"}),
        );
        assert_eq!(status, 200, "{body}");
        assert!(
            body["columns"]
                .as_array()
                .unwrap()
                .iter()
                .any(|column| column == "p.bio"),
            "evolved query must project the new field: {body}"
        );
    }

    // Out-of-band drift: the live graph evolves behind the cluster's back;
    // refresh observes it, apply converges it back to the declared schema.
    std::fs::write(
        dir.join("rogue.pg"),
        "\nnode Person {\n  name: String @key\n  bio: String?\n  rogue: String?\n}\n",
    )
    .unwrap();
    let output = cli()
        .arg("schema")
        .arg("apply")
        .arg(dir.join("graphs/knowledge.omni"))
        .arg("--schema")
        .arg(dir.join("rogue.pg"))
        .arg("--json")
        .output()
        .unwrap();
    assert!(output.status.success(), "out-of-band schema apply failed");
    let refresh = cluster_cli(dir, &["refresh"]);
    assert_eq!(
        refresh["resource_statuses"]["schema.knowledge"]["status"],
        "drifted",
        "{refresh}"
    );
    let heal = cluster_cli(dir, &["apply"]);
    assert_eq!(heal["converged"], true, "{heal}");
    let schema_show = cli()
        .arg("schema")
        .arg("show")
        .arg(dir.join("graphs/knowledge.omni"))
        .output()
        .unwrap();
    assert!(
        schema_show.status.success(),
        "schema show failed: {}",
        String::from_utf8_lossy(&schema_show.stderr)
    );
    let shown = String::from_utf8_lossy(&schema_show.stdout);
    assert!(shown.contains("Person"), "schema show produced no schema: {shown}");
    assert!(
        !shown.contains("rogue"),
        "drift must be soft-dropped back to the declared schema: {shown}"
    );

    // Retire engineering: gated delete, then the server serves the survivor.
    std::fs::write(
        dir.join("cluster.yaml"),
        r#"
version: 1
graphs:
  knowledge:
    schema: ./people.pg
    queries:
      find_person:
        file: ./people.gq
"#,
    )
    .unwrap();
    let blocked = cluster_cli(dir, &["apply"]);
    assert_eq!(blocked["converged"], false, "{blocked}");
    let approve_output = cli()
        .arg("--as")
        .arg("andrew")
        .arg("cluster")
        .arg("approve")
        .arg("graph.engineering")
        .arg("--config")
        .arg(dir)
        .arg("--json")
        .output()
        .unwrap();
    assert!(approve_output.status.success(), "approve failed");
    let delete = cluster_cli(dir, &["apply"]);
    assert_eq!(delete["converged"], true, "{delete}");
    assert!(!dir.join("graphs/engineering.omni").exists());

    {
        let server = spawn_server_with_cluster(dir);
        let (status, body) = invoke_query(
            &client,
            &server.base_url,
            "knowledge",
            "find_person",
            serde_json::json!({"name": "Ada"}),
        );
        assert_eq!(status, 200, "{body}");
        let response = client
            .post(format!(
                "{}/graphs/engineering/queries/find_service",
                server.base_url
            ))
            .json(&serde_json::json!({"params": {"name": "billing"}}))
            .send()
            .unwrap();
        assert_eq!(
            response.status().as_u16(),
            404,
            "a deleted graph must vanish from the serving surface"
        );
    }

    // The story ends converged: nothing left to do.
    let final_plan = cluster_cli(dir, &["plan"]);
    assert!(
        final_plan["changes"].as_array().unwrap().is_empty(),
        "{final_plan}"
    );
}

/// Applied policy bundles gate serving per their bindings: the cluster-bound
/// bundle owns the management surface (graph_list), the graph-bound bundle
/// owns query invocation — enforced over HTTP with bearer-resolved actors.
#[test]
fn local_cluster_serving_enforces_applied_policy_bindings() {
    if skip_system_e2e("local_cluster_serving_enforces_applied_policy_bindings") {
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path();
    std::fs::write(
        dir.join("people.pg"),
        "\nnode Person {\n  name: String @key\n}\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("people.gq"),
        "\nquery find_person($name: String) {\n  match { $p: Person { name: $name } }\n  return { $p.name }\n}\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("graph.policy.yaml"),
        r#"
version: 1
groups:
  readers: ["act-reader"]
protected_branches: [main]
rules:
  - id: allow-invoke
    allow:
      actors: { group: readers }
      actions: [invoke_query]
  - id: allow-read
    allow:
      actors: { group: readers }
      actions: [read]
      branch_scope: any
"#,
    )
    .unwrap();
    std::fs::write(
        dir.join("server.policy.yaml"),
        r#"
version: 1
kind: server
groups:
  admins: ["act-admin"]
rules:
  - id: allow-list
    allow:
      actors: { group: admins }
      actions: [graph_list]
"#,
    )
    .unwrap();
    std::fs::write(
        dir.join("cluster.yaml"),
        r#"
version: 1
graphs:
  knowledge:
    schema: ./people.pg
    queries:
      find_person:
        file: ./people.gq
policies:
  graph_rules:
    file: ./graph.policy.yaml
    applies_to: [knowledge]
  server_rules:
    file: ./server.policy.yaml
    applies_to: [cluster]
"#,
    )
    .unwrap();
    assert_eq!(cluster_cli(dir, &["import"])["ok"], true);
    let converge = cluster_cli(dir, &["apply"]);
    assert_eq!(converge["converged"], true, "{converge}");
    seed_graph(dir, "knowledge", "{\"type\":\"Person\",\"data\":{\"name\":\"Ada\"}}\n");

    let server = spawn_server_with_cluster_env(
        dir,
        &[(
            "OMNIGRAPH_SERVER_BEARER_TOKENS_JSON",
            r#"{"act-admin":"admin-token","act-reader":"reader-token"}"#,
        )],
    );
    let client = Client::new();
    let get_graphs = |token: Option<&str>| {
        let mut request = client.get(format!("{}/graphs", server.base_url));
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        request.send().unwrap().status().as_u16()
    };
    // Management surface: cluster-bound bundle, admins only.
    assert_eq!(get_graphs(Some("admin-token")), 200);
    assert_eq!(get_graphs(Some("reader-token")), 403);
    assert_eq!(get_graphs(None), 401);

    // Query invocation: graph-bound bundle, readers only.
    let invoke = |token: &str| {
        client
            .post(format!(
                "{}/graphs/knowledge/queries/find_person",
                server.base_url
            ))
            .bearer_auth(token)
            .json(&serde_json::json!({"params": {"name": "Ada"}}))
            .send()
            .unwrap()
    };
    let response = invoke("reader-token");
    assert_eq!(response.status().as_u16(), 200);
    let body: serde_json::Value = response.json().unwrap();
    assert_eq!(body["rows"][0]["p.name"], "Ada", "{body}");
    // Denied invocation is deliberately 404, indistinguishable from an
    // unknown query — the server's anti-probing contract.
    assert_eq!(invoke("admin-token").status().as_u16(), 404);
}

/// Rule 0 (axiom 15): a --cluster server never reads omnigraph.yaml — not
/// even the implicit cwd search. A MALFORMED config in the process cwd must
/// not affect boot or serving.
#[test]
fn cluster_server_boot_ignores_local_config_in_cwd() {
    let cluster = tempfile::tempdir().unwrap();
    std::fs::write(
        cluster.path().join("people.pg"),
        "\nnode Person {\n  name: String @key\n}\n",
    )
    .unwrap();
    std::fs::write(
        cluster.path().join("cluster.yaml"),
        "version: 1\ngraphs:\n  knowledge:\n    schema: ./people.pg\n",
    )
    .unwrap();
    for command in ["import", "apply"] {
        let output = cli()
            .arg("cluster")
            .arg(command)
            .arg("--config")
            .arg(cluster.path())
            .output()
            .unwrap();
        assert!(output.status.success(), "cluster {command} failed");
    }
    let cwd = tempfile::tempdir().unwrap();
    std::fs::write(cwd.path().join("omnigraph.yaml"), "{{{{ not yaml").unwrap();

    let server = spawn_server_with_cluster_in(cluster.path(), cwd.path());
    let response = reqwest::blocking::get(format!("{}/healthz", server.base_url)).unwrap();
    assert!(response.status().is_success());
}
