mod support;

use std::env;
use std::fs;

use omnigraph::db::Omnigraph;
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
            .arg("--mode")
            .arg("overwrite")
            .arg("--data")
            .arg(fixture("test.jsonl"))
            .arg(graph.path()),
    );

    let read_before = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg("--store")
            .arg(graph.path())
            .arg("--query")
            .arg(fixture("test.gq"))
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
            .arg("--store")
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
            .arg("--store")
            .arg(graph.path())
            .arg("--query")
            .arg(fixture("test.gq"))
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
            .arg("--store")
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
            .arg("--store")
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
            .arg("--store")
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
            .arg("--store")
            .arg(graph.path())
            .arg("--query")
            .arg(fixture("test.gq"))
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
            .arg("--store")
            .arg(graph.path())
            .arg("--query")
            .arg(fixture("test.gq"))
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

    let ingest_output = output_success(
        cli()
            .arg("ingest")
            .arg("--data")
            .arg(&ingest_data)
            .arg("--branch")
            .arg("feature-ingest")
            .arg(graph.path())
            .arg("--json"),
    );
    // The deprecation warning goes to stderr so --json stdout stays clean.
    assert!(
        String::from_utf8_lossy(&ingest_output.stderr).contains("deprecated"),
        "ingest must warn about its deprecation on stderr"
    );
    let ingest_payload = parse_stdout_json(&ingest_output);
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
            .arg("--store")
            .arg(graph.path())
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

    let bob = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg("--store")
            .arg(graph.path())
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
}

/// The unified `load` subsumes ingest: `--from` opts into fork-if-missing,
/// while without it a missing branch is an error — never an implicit fork.
#[test]
fn local_cli_load_from_forks_branch_and_missing_branch_errors_without_from() {
    let graph = SystemGraph::loaded();
    let extra = graph.write_jsonl(
        "system-local-load-from.jsonl",
        r#"{"type":"Person","data":{"name":"Zoe","age":33}}"#,
    );

    // Without --from, a missing branch must fail and create nothing.
    let failure = output_failure(
        cli()
            .arg("load")
            .arg("--mode")
            .arg("merge")
            .arg("--data")
            .arg(&extra)
            .arg("--branch")
            .arg("feature-load")
            .arg(graph.path()),
    );
    assert!(
        String::from_utf8_lossy(&failure.stderr).contains("feature-load"),
        "error should name the missing branch"
    );

    // With --from, the branch is forked and the load lands on it.
    let payload = parse_stdout_json(&output_success(
        cli()
            .arg("load")
            .arg("--mode")
            .arg("merge")
            .arg("--data")
            .arg(&extra)
            .arg("--branch")
            .arg("feature-load")
            .arg("--from")
            .arg("main")
            .arg(graph.path())
            .arg("--json"),
    ));
    assert_eq!(payload["branch"], "feature-load");
    assert_eq!(payload["base_branch"], "main");
    assert_eq!(payload["branch_created"], true);
    assert_eq!(payload["mode"], "merge");
    assert_eq!(payload["nodes_loaded"], 1);

    let snapshot = parse_stdout_json(&output_success(
        cli()
            .arg("snapshot")
            .arg(graph.path())
            .arg("--branch")
            .arg("feature-load")
            .arg("--json"),
    ));
    assert_eq!(snapshot["branch"], "feature-load");
}

/// `--mode` is required: overwrite is destructive, so the unified `load`
/// has no implicit default.
#[test]
fn local_cli_load_requires_mode_flag() {
    let graph = SystemGraph::loaded();
    let extra = graph.write_jsonl(
        "system-local-load-no-mode.jsonl",
        r#"{"type":"Person","data":{"name":"Zoe","age":33}}"#,
    );

    let failure = output_failure(
        cli()
            .arg("load")
            .arg("--data")
            .arg(&extra)
            .arg(graph.path()),
    );
    assert!(
        String::from_utf8_lossy(&failure.stderr).contains("--mode"),
        "clap should demand the missing --mode flag"
    );
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
            .arg("--mode")
            .arg("overwrite")
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

    let friends = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg("--store")
            .arg(&imported_graph)
            .arg("--query")
            .arg(fixture("test.gq"))
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
    let query = query_root.join("test.gq");
    fs::copy(fixture("test.gq"), &query).unwrap();

    output_success(
        cli()
            .current_dir(query_root)
            .arg("init")
            .arg("--schema")
            .arg(fixture("test.pg"))
            .arg(&graph_uri),
    );
    output_success(
        cli()
            .current_dir(query_root)
            .arg("load")
            .arg("--mode")
            .arg("overwrite")
            // `--yes` clears the RFC-011 Decision 9 destructive-write
            // confirmation: `--mode overwrite` against a non-local (s3://)
            // target is refused without it.
            .arg("--yes")
            .arg("--data")
            .arg(fixture("test.jsonl"))
            .arg(&graph_uri),
    );

    // RFC-011: the graph is addressed by `--store <uri>`; the `.gq` path is
    // resolved cwd-relative (no omnigraph.yaml `query.roots`).
    let read = parse_stdout_json(&output_success(
        cli()
            .current_dir(query_root)
            .arg("read")
            .arg("--store")
            .arg(&graph_uri)
            .arg("--query")
            .arg("test.gq")
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
            .arg("--store")
            .arg(&graph_uri)
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
            .arg("--store")
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
            .arg("--store")
            .arg(graph.path())
            .arg("--query")
            .arg(fixture("test.gq"))
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
fn local_cli_resolves_relative_query_cwd_relative() {
    // RFC-011: omnigraph.yaml `query.roots` search is gone — a `--query`
    // path is resolved plainly relative to the process cwd. This pins that
    // a bare relative `.gq` filename resolves against `.current_dir`, and
    // that the file actually read is the cwd-local one (a same-named query
    // elsewhere with different columns is never picked up).
    let graph = SystemGraph::loaded();
    let root = graph.path().parent().unwrap();
    let cwd_dir = root.join("cwd");
    let other_dir = root.join("other");
    fs::create_dir_all(&cwd_dir).unwrap();
    fs::create_dir_all(&other_dir).unwrap();

    // The query in the cwd projects (age, name).
    write_query_file(
        &cwd_dir.join("local.gq"),
        r#"
query get_person($name: String) {
    match {
        $p: Person { name: $name }
    }
    return { $p.age, $p.name }
}
"#,
    );
    // A same-named query elsewhere projects only (name): if cwd-relative
    // resolution regressed and picked this up, the columns assert fails.
    write_query_file(
        &other_dir.join("local.gq"),
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
            .current_dir(&cwd_dir)
            .arg("read")
            .arg("--store")
            .arg(graph.path())
            .arg("--query")
            .arg("local.gq")
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
    output_success(
        cli()
            .arg("load")
            .arg("--mode")
            .arg("overwrite")
            .arg("--data")
            .arg(&data)
            .arg(&graph),
    );

    let filtered = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg("--store")
            .arg(&graph)
            .arg("--query")
            .arg(&queries)
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
            .arg("--store")
            .arg(&graph)
            .arg("--query")
            .arg(&queries)
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
            .arg("--store")
            .arg(&graph)
            .arg("--query")
            .arg(&queries)
            .arg("update_task")
            .arg("--params")
            .arg(r#"{"slug":"gamma","due_at":"2026-04-04T10:45:00Z","tags":["embed","released"],"scores":[13,21],"active_days":["2026-04-04","2026-04-05"]}"#)
            .arg("--json"),
    ));
    assert_eq!(update_payload["affected_nodes"], 1);

    let gamma = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg("--store")
            .arg(&graph)
            .arg("--query")
            .arg(&queries)
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
    output_success(
        cli()
            .arg("load")
            .arg("--mode")
            .arg("overwrite")
            .arg("--data")
            .arg(&data)
            .arg(&graph),
    );

    let result = parse_stdout_json(&output_success(
        cli()
            // Stored vectors above were produced with gemini-embedding-2-preview;
            // pin the query-time embedder to the same provider/model so the
            // auto-embedded `$q` lands in the same vector space.
            .env("OMNIGRAPH_EMBED_PROVIDER", "gemini")
            .env("OMNIGRAPH_EMBED_MODEL", "gemini-embedding-2-preview")
            .arg("read")
            .arg("--store")
            .arg(&graph)
            .arg("--query")
            .arg(&queries)
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
    // RFC-011: the read-only policy CLI surfaces source the bundle from a
    // cluster's applied policies (`--cluster <dir>` + `--graph <id>`), not
    // from an omnigraph.yaml `graphs:` map. These don't mutate the graph;
    // they parse and evaluate the effective bundle bound to the graph.
    let cluster = converged_loaded_cluster("knowledge", Some(POLICY_E2E_YAML));
    // `policy test` has no per-bundle tests file in the cluster model, so
    // the cases are supplied explicitly via `--tests`.
    let tests_file = cluster.path().join("policy.tests.yaml");
    fs::write(&tests_file, POLICY_E2E_TESTS_YAML).unwrap();

    let validate = output_success(
        cli()
            .arg("policy")
            .arg("validate")
            .arg("--cluster")
            .arg(cluster.path())
            .arg("--graph")
            .arg("knowledge"),
    );
    assert!(stdout_string(&validate).contains("policy valid:"));

    let tests = output_success(
        cli()
            .arg("policy")
            .arg("test")
            .arg("--cluster")
            .arg(cluster.path())
            .arg("--graph")
            .arg("knowledge")
            .arg("--tests")
            .arg(&tests_file),
    );
    assert!(stdout_string(&tests).contains("policy tests passed: 2 cases"));

    let explain = output_success(
        cli()
            .arg("policy")
            .arg("explain")
            .arg("--cluster")
            .arg(cluster.path())
            .arg("--graph")
            .arg("knowledge")
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

/// Token→actor map for the served-policy tests: the bearer tokens the
/// cluster server resolves to `act-bruno` / `act-ragnor`.
const POLICY_TOKENS_JSON: &str = r#"{"act-bruno":"bruno-tok","act-ragnor":"ragnor-tok"}"#;

#[test]
fn local_cli_change_enforces_engine_layer_policy() {
    // RFC-011: a CLI direct-store write carries NO policy — policy lives in
    // the cluster/server. So engine-layer policy on a direct write no longer
    // exists; this test asserts the faithful migration: the SERVER enforces
    // the bundle bound to the served graph, addressed via `--server --graph`
    // with a bearer token that resolves to the actor.
    //
    // Three cases, each discriminating:
    //
    // 1. No token → the server refuses (401, unauthenticated). The old
    //    embedded "no actor" footgun does not apply to the served path
    //    (the actor comes from the token), so this replaces it.
    // 2. bruno token, change on protected main → Cedar denies (bruno can
    //    change unprotected branches; main is protected). Non-zero exit,
    //    "denied" surfaced from the server error body.
    // 3. ragnor token, change on main → Cedar permits (admins-write). Write
    //    succeeds and the inserted row is readable.
    if skip_system_e2e("local_cli_change_enforces_engine_layer_policy") {
        return;
    }
    let cluster = converged_loaded_cluster("knowledge", Some(POLICY_E2E_YAML));
    let server = spawn_server_with_cluster_env(
        cluster.path(),
        &[("OMNIGRAPH_SERVER_BEARER_TOKENS_JSON", POLICY_TOKENS_JSON)],
    );
    let insert =
        "query add($name: String, $age: I32) { insert Person { name: $name, age: $age } }";

    // Case 1: no token → the server refuses before any policy check.
    let no_token = cli()
        .arg("change")
        .arg("--server")
        .arg(&server.base_url)
        .arg("--graph")
        .arg("knowledge")
        .arg("-e")
        .arg(insert)
        .arg("--params")
        .arg(r#"{"name":"NoTokenPerson","age":1}"#)
        .arg("--json")
        .output()
        .unwrap();
    assert!(
        !no_token.status.success(),
        "unauthenticated served write must be refused: {no_token:?}"
    );

    // Case 2: bruno token against protected main → denied by the server.
    let denied = cli()
        .env("OMNIGRAPH_BEARER_TOKEN", "bruno-tok")
        .arg("change")
        .arg("--server")
        .arg(&server.base_url)
        .arg("--graph")
        .arg("knowledge")
        .arg("-e")
        .arg(insert)
        .arg("--params")
        .arg(r#"{"name":"BrunoOnMain","age":2}"#)
        .arg("--json")
        .output()
        .unwrap();
    assert!(!denied.status.success(), "bruno/main must be denied");
    let denied_stderr = String::from_utf8_lossy(&denied.stderr);
    assert!(
        denied_stderr.contains("denied"),
        "expected 'denied' message for bruno/main, got stderr: {denied_stderr}"
    );

    // Case 3: ragnor token against main → permitted by admins-write.
    let allowed = parse_stdout_json(&output_success(
        cli()
            .env("OMNIGRAPH_BEARER_TOKEN", "ragnor-tok")
            .arg("change")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg("knowledge")
            .arg("-e")
            .arg(insert)
            .arg("--params")
            .arg(r#"{"name":"RagnorOnMain","age":3}"#)
            .arg("--json"),
    ));
    assert_eq!(allowed["branch"], "main");
    assert_eq!(allowed["affected_nodes"], 1);
    assert_eq!(allowed["actor_id"], "act-ragnor");

    // Verify the row landed — proves the write actually committed, not
    // just that enforce returned Ok and silently dropped the work. The read
    // uses the bruno token: POLICY_E2E_YAML grants `read` to the `team`
    // group (bruno), while admins (ragnor) get write-only rules.
    let verify = parse_stdout_json(&output_success(
        cli()
            .env("OMNIGRAPH_BEARER_TOKEN", "bruno-tok")
            .arg("read")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg("knowledge")
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"RagnorOnMain"}"#)
            .arg("--json"),
    ));
    assert_eq!(verify["row_count"], 1);
    assert_eq!(verify["rows"][0]["p.name"], "RagnorOnMain");
}

#[test]
fn local_cli_direct_store_write_is_unpoliced_regardless_of_actor() {
    // RFC-011: a direct (`--store`) write carries no Cedar policy at all —
    // policy lives in the cluster/server. So a write that the SERVED path
    // would deny (bruno changing protected main) succeeds on the direct
    // path, regardless of the actor. This is the faithful replacement for
    // the obsolete `..._positional_uri_does_not_inherit_default_graph_policy`
    // premise: a positional/`--store` address has no policy to inherit.
    let graph = SystemGraph::loaded();
    let mutation_file = insert_person_query(&graph, "system-local-policy-direct.gq");

    let allowed = parse_stdout_json(&output_success(
        cli()
            .arg("--as")
            .arg("act-bruno")
            .arg("change")
            .arg("--store")
            .arg(graph.path())
            .arg("--query")
            .arg(&mutation_file)
            .arg("--params")
            .arg(r#"{"name":"DirectStoreBruno","age":4}"#)
            .arg("--json"),
    ));
    assert_eq!(allowed["branch"], "main");
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
    // RFC-011 served re-point: the server enforces the graph-bound bundle on
    // a remote load. A load into protected main is a `change`: bruno
    // (team-write-unprotected) is denied, ragnor (admins-write) is allowed.
    if skip_system_e2e("local_cli_load_enforces_engine_layer_policy") {
        return;
    }
    let cluster = converged_loaded_cluster("knowledge", Some(POLICY_E2E_YAML));
    let server = spawn_server_with_cluster_env(
        cluster.path(),
        &[("OMNIGRAPH_SERVER_BEARER_TOKENS_JSON", POLICY_TOKENS_JSON)],
    );
    let temp = tempfile::tempdir().unwrap();
    let data = temp.path().join("policy-load.jsonl");
    // The seeded graph (test.jsonl) has Knows/WorksAt edges over its Persons, so a
    // per-table overwrite must be self-consistent: replacing node:Person also
    // replaces the edge tables that referenced the old Persons, or the retained
    // edges would strand against the new node image (a loud OrphanEdge). The data
    // is incidental to this policy test; it just has to commit cleanly for the
    // allowed actor. WorksAt points at Acme, a Company the overwrite retains.
    fs::write(
        &data,
        "{\"type\":\"Person\",\"data\":{\"name\":\"LoadPolicy\",\"age\":11}}\n\
         {\"edge\":\"Knows\",\"from\":\"LoadPolicy\",\"to\":\"LoadPolicy\"}\n\
         {\"edge\":\"WorksAt\",\"from\":\"LoadPolicy\",\"to\":\"Acme\"}\n",
    )
    .unwrap();

    // act-bruno: change-on-protected is denied (team-write-unprotected only).
    let denied = cli()
        .env("OMNIGRAPH_BEARER_TOKEN", "bruno-tok")
        .arg("load")
        .arg("--mode")
        .arg("overwrite")
        // `--yes` clears the RFC-011 Decision 9 destructive-write confirmation
        // so the policy check (not the confirmation refusal) is what denies.
        .arg("--yes")
        .arg("--server")
        .arg(&server.base_url)
        .arg("--graph")
        .arg("knowledge")
        .arg("--data")
        .arg(&data)
        .arg("--json")
        .output()
        .unwrap();
    assert!(!denied.status.success(), "bruno/main load must be denied");
    let stderr = String::from_utf8_lossy(&denied.stderr);
    assert!(
        stderr.contains("denied"),
        "expected 'denied' for bruno/main load, got: {stderr}"
    );

    // act-ragnor: admins-write rule permits change anywhere.
    let allowed = parse_stdout_json(&output_success(
        cli()
            .env("OMNIGRAPH_BEARER_TOKEN", "ragnor-tok")
            .arg("load")
            .arg("--mode")
            .arg("overwrite")
            .arg("--yes")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg("knowledge")
            .arg("--data")
            .arg(&data)
            .arg("--json"),
    ));
    assert_eq!(allowed["branch"], "main");
    assert!(allowed["nodes_loaded"].as_u64().unwrap() >= 1);
}

#[test]
fn local_cli_ingest_enforces_engine_layer_policy() {
    // RFC-011 served re-point: ingest into a new branch requires both
    // BranchCreate and Change. Bruno has change-unprotected only (no
    // branch-ops) — either gate denies. Ragnor has admins-write +
    // admins-branch-ops — both fire as ingest creates the branch + loads.
    if skip_system_e2e("local_cli_ingest_enforces_engine_layer_policy") {
        return;
    }
    let cluster = converged_loaded_cluster("knowledge", Some(POLICY_E2E_YAML));
    let server = spawn_server_with_cluster_env(
        cluster.path(),
        &[("OMNIGRAPH_SERVER_BEARER_TOKENS_JSON", POLICY_TOKENS_JSON)],
    );
    let temp = tempfile::tempdir().unwrap();
    let data = temp.path().join("policy-ingest.jsonl");
    fs::write(
        &data,
        r#"{"type":"Person","data":{"name":"IngestPolicy","age":12}}"#,
    )
    .unwrap();

    let denied = cli()
        .env("OMNIGRAPH_BEARER_TOKEN", "bruno-tok")
        .arg("ingest")
        .arg("--server")
        .arg(&server.base_url)
        .arg("--graph")
        .arg("knowledge")
        .arg("--data")
        .arg(&data)
        .arg("--branch")
        .arg("policy-ingest-feature")
        .arg("--json")
        .output()
        .unwrap();
    assert!(!denied.status.success(), "bruno ingest must be denied");
    let stderr = String::from_utf8_lossy(&denied.stderr);
    assert!(
        stderr.contains("denied"),
        "expected 'denied' for bruno ingest, got: {stderr}"
    );

    let allowed = parse_stdout_json(&output_success(
        cli()
            .env("OMNIGRAPH_BEARER_TOKEN", "ragnor-tok")
            .arg("ingest")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg("knowledge")
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
fn local_cli_branch_create_enforces_engine_layer_policy() {
    // RFC-011 served re-point: bruno has no branch-ops rule → denied;
    // ragnor has admins-branch-ops → allowed.
    if skip_system_e2e("local_cli_branch_create_enforces_engine_layer_policy") {
        return;
    }
    let cluster = converged_loaded_cluster("knowledge", Some(POLICY_E2E_YAML));
    let server = spawn_server_with_cluster_env(
        cluster.path(),
        &[("OMNIGRAPH_SERVER_BEARER_TOKENS_JSON", POLICY_TOKENS_JSON)],
    );

    let denied = cli()
        .env("OMNIGRAPH_BEARER_TOKEN", "bruno-tok")
        .arg("branch")
        .arg("create")
        .arg("--server")
        .arg(&server.base_url)
        .arg("--graph")
        .arg("knowledge")
        .arg("--from")
        .arg("main")
        .arg("bruno-feature")
        .output()
        .unwrap();
    assert!(!denied.status.success(), "bruno branch create must be denied");
    let stderr = String::from_utf8_lossy(&denied.stderr);
    assert!(
        stderr.contains("denied"),
        "expected 'denied' for bruno branch create, got: {stderr}"
    );

    output_success(
        cli()
            .env("OMNIGRAPH_BEARER_TOKEN", "ragnor-tok")
            .arg("branch")
            .arg("create")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg("knowledge")
            .arg("--from")
            .arg("main")
            .arg("ragnor-feature"),
    );
}

#[test]
fn local_cli_branch_delete_enforces_engine_layer_policy() {
    // RFC-011 served re-point: bruno has no branch-ops rule → denied;
    // ragnor has admins-branch-ops → allowed.
    if skip_system_e2e("local_cli_branch_delete_enforces_engine_layer_policy") {
        return;
    }
    let cluster = converged_loaded_cluster("knowledge", Some(POLICY_E2E_YAML));
    let server = spawn_server_with_cluster_env(
        cluster.path(),
        &[("OMNIGRAPH_SERVER_BEARER_TOKENS_JSON", POLICY_TOKENS_JSON)],
    );

    // Pre-create the branch as ragnor so there's something to delete.
    output_success(
        cli()
            .env("OMNIGRAPH_BEARER_TOKEN", "ragnor-tok")
            .arg("branch")
            .arg("create")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg("knowledge")
            .arg("--from")
            .arg("main")
            .arg("doomed"),
    );

    // `--yes` clears the RFC-011 Decision 9 destructive-write confirmation so
    // the policy check (not the confirmation refusal) is what denies.
    let denied = cli()
        .env("OMNIGRAPH_BEARER_TOKEN", "bruno-tok")
        .arg("branch")
        .arg("delete")
        .arg("--yes")
        .arg("--server")
        .arg(&server.base_url)
        .arg("--graph")
        .arg("knowledge")
        .arg("doomed")
        .output()
        .unwrap();
    assert!(!denied.status.success(), "bruno branch delete must be denied");
    let stderr = String::from_utf8_lossy(&denied.stderr);
    assert!(
        stderr.contains("denied"),
        "expected 'denied' for bruno branch delete, got: {stderr}"
    );

    output_success(
        cli()
            .env("OMNIGRAPH_BEARER_TOKEN", "ragnor-tok")
            .arg("branch")
            .arg("delete")
            .arg("--yes")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg("knowledge")
            .arg("doomed"),
    );
}

#[test]
fn local_cli_branch_merge_enforces_engine_layer_policy() {
    // RFC-011 served re-point: merging into protected main needs
    // branch_merge with target_branch_scope protected. bruno has no such
    // rule → denied; ragnor has admins-promote → allowed.
    if skip_system_e2e("local_cli_branch_merge_enforces_engine_layer_policy") {
        return;
    }
    let cluster = converged_loaded_cluster("knowledge", Some(POLICY_E2E_YAML));
    let server = spawn_server_with_cluster_env(
        cluster.path(),
        &[("OMNIGRAPH_SERVER_BEARER_TOKENS_JSON", POLICY_TOKENS_JSON)],
    );

    // Pre-create a feature branch as ragnor (admins-branch-ops covers it).
    output_success(
        cli()
            .env("OMNIGRAPH_BEARER_TOKEN", "ragnor-tok")
            .arg("branch")
            .arg("create")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg("knowledge")
            .arg("--from")
            .arg("main")
            .arg("merge-feature"),
    );

    let denied = cli()
        .env("OMNIGRAPH_BEARER_TOKEN", "bruno-tok")
        .arg("branch")
        .arg("merge")
        .arg("--server")
        .arg(&server.base_url)
        .arg("--graph")
        .arg("knowledge")
        .arg("merge-feature")
        .arg("--into")
        .arg("main")
        .output()
        .unwrap();
    assert!(!denied.status.success(), "bruno branch merge must be denied");
    let stderr = String::from_utf8_lossy(&denied.stderr);
    assert!(
        stderr.contains("denied"),
        "expected 'denied' for bruno branch merge, got: {stderr}"
    );

    output_success(
        cli()
            .env("OMNIGRAPH_BEARER_TOKEN", "ragnor-tok")
            .arg("branch")
            .arg("merge")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg("knowledge")
            .arg("merge-feature")
            .arg("--into")
            .arg("main"),
    );
}

// ─── RFC-011: operator.actor cascade ──────────────────────────────────────
//
// The CLI actor chain is `--as` > `operator.actor` (in the operator config
// at $OMNIGRAPH_HOME/config.yaml) > none. These two tests pin that order on
// a direct (`--store`) write. RFC-011 makes direct-store writes unpoliced,
// so the assertion is on which `actor_id` the write records, not on a Cedar
// allow/deny — the actor still has to be resolved correctly and stamped onto
// the commit.

/// An operator config (`$OMNIGRAPH_HOME/config.yaml`) carrying just
/// `operator.actor`. Pointing OMNIGRAPH_HOME at the holding dir makes the
/// CLI read it as the operator layer.
fn operator_home_with_actor(actor: &str) -> tempfile::TempDir {
    let home = tempfile::tempdir().unwrap();
    fs::write(
        home.path().join("config.yaml"),
        format!("operator:\n  actor: {actor}\n"),
    )
    .unwrap();
    home
}

#[test]
fn local_cli_actor_from_config_used_when_no_flag() {
    // operator.actor: act-ragnor in the operator config, no --as flag →
    // the write records act-ragnor. Proves the operator-layer actor source
    // is consulted when `--as` is absent.
    let graph = SystemGraph::loaded();
    let home = operator_home_with_actor("act-ragnor");
    let mutation_file = insert_person_query(&graph, "system-local-cli-actor.gq");

    let allowed = parse_stdout_json(&output_success(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .arg("change")
            .arg("--store")
            .arg(graph.path())
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
    // operator.actor: act-ragnor in the config + --as act-bruno on the CLI →
    // the write records act-bruno. The flag wins per the precedence rule.
    // Without this test, a future change that reverses precedence would ride
    // through silently.
    let graph = SystemGraph::loaded();
    let home = operator_home_with_actor("act-ragnor");
    let mutation_file = insert_person_query(&graph, "system-local-cli-actor-override.gq");

    let overridden = parse_stdout_json(&output_success(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .arg("--as")
            .arg("act-bruno")
            .arg("change")
            .arg("--store")
            .arg(graph.path())
            .arg("--query")
            .arg(&mutation_file)
            .arg("--params")
            .arg(r#"{"name":"OverrideEve","age":19}"#)
            .arg("--json"),
    ));
    assert_eq!(overridden["affected_nodes"], 1);
    assert_eq!(overridden["actor_id"], "act-bruno");
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
        .arg("--mode")
        .arg("overwrite")
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
        .arg("--mode")
        .arg("overwrite")
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
    // refresh observes it, apply converges it back to the declared schema. RFC-011
    // D10 makes the CLI `schema apply` refuse a cluster-managed graph, so a true
    // bypass is a direct engine apply against the storage root.
    let rogue_pg = "\nnode Person {\n  name: String @key\n  bio: String?\n  rogue: String?\n}\n";
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let db = Omnigraph::open(dir.join("graphs/knowledge.omni").to_string_lossy().as_ref())
            .await
            .unwrap();
        db.apply_schema(rogue_pg).await.unwrap();
    });
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

/// RFC-007 PR 2: keyed credentials end to end — `login` stores a 0600
/// credential, the URL-matched server's token chain authenticates remote
/// reads (env > file), a non-matching URL never sees the token (§D5 rule
/// 3), and `logout` revokes.
#[test]
fn local_cli_keyed_credentials_authenticate_url_matched_server() {
    // RFC-011 cluster-only: the server boots from a converged cluster
    // serving the fixture graph under id `local`; tokens-only boot is
    // default-deny, which still permits `read`.
    let cluster = converged_loaded_cluster("local", None);
    let server = spawn_server_with_cluster_env(
        cluster.path(),
        &[("OMNIGRAPH_SERVER_BEARER_TOKEN", "secret-tok")],
    );
    let operator_home = tempfile::tempdir().unwrap();
    let write_server_url = |url: &str| {
        fs::write(
            operator_home.path().join("config.yaml"),
            format!("servers:\n  test-srv:\n    url: {url}\n"),
        )
        .unwrap();
    };
    write_server_url(&server.base_url);

    let remote_read = |envs: &[(&str, &str)]| {
        let mut command = cli();
        command.env("OMNIGRAPH_HOME", operator_home.path());
        for (name, value) in envs {
            command.env(name, value);
        }
        command
            .arg("read")
            .arg("--server")
            .arg(&server.base_url)
            .arg("--graph")
            .arg("local")
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"Alice"}"#)
            .arg("--json")
            .output()
            .unwrap()
    };

    // No credential anywhere: the server refuses.
    let output = remote_read(&[]);
    assert!(!output.status.success(), "{output:?}");

    // login with a WRONG token (via stdin, the documented pipe flow).
    let output = cli()
        .env("OMNIGRAPH_HOME", operator_home.path())
        .arg("login")
        .arg("test-srv")
        .write_stdin("wrong-tok\n")
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let output = remote_read(&[]);
    assert!(!output.status.success(), "wrong token must not authenticate");

    // Re-login rotates to the right token (via --token); 0600 on disk.
    let output = cli()
        .env("OMNIGRAPH_HOME", operator_home.path())
        .arg("login")
        .arg("test-srv")
        .arg("--token")
        .arg("secret-tok")
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let credentials = operator_home.path().join("credentials");
    let text = fs::read_to_string(&credentials).unwrap();
    assert!(text.contains("[test-srv]"), "{text}");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(&credentials).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "{:o}", mode & 0o777);
    }
    let output = remote_read(&[]);
    assert!(
        output.status.success(),
        "keyed credential must authenticate the URL-matched server: {output:?}"
    );
    let payload: serde_json::Value =
        serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["rows"][0]["p.name"], "Alice");

    // OMNIGRAPH_TOKEN_<NAME> env outranks the credentials file.
    let output = remote_read(&[("OMNIGRAPH_TOKEN_TEST_SRV", "env-wrong")]);
    assert!(
        !output.status.success(),
        "keyed env token must outrank the credentials file"
    );

    // §D5 rule 3: a URL matching no operator server never sees the token.
    write_server_url("http://127.0.0.1:1");
    let output = remote_read(&[]);
    assert!(
        !output.status.success(),
        "token keyed to another url must not be sent here"
    );
    write_server_url(&server.base_url);

    // logout revokes; idempotent.
    for _ in 0..2 {
        let output = cli()
            .env("OMNIGRAPH_HOME", operator_home.path())
            .arg("logout")
            .arg("test-srv")
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
    }
    let output = remote_read(&[]);
    assert!(!output.status.success(), "logout must revoke access");
}

/// RFC-007 PR 3: --server targeting and operator aliases (pure bindings to
/// stored queries) end to end, with the keyed credential from PR 2.
#[test]
fn local_cli_operator_alias_and_server_flag_invoke_stored_query() {
    // RFC-011 cluster-only: build a converged cluster serving graph `local`
    // with a stored query `find_person` and a per-graph policy granting the
    // operator invoke_query + read (invoke_query is policy-gated — anti-probing
    // 404 without the grant).
    let cluster = tempfile::tempdir().unwrap();
    fs::copy(fixture("test.pg"), cluster.path().join("local.pg")).unwrap();
    fs::write(
        cluster.path().join("find-person.gq"),
        "query find_person($name: String) { match { $p: Person { name: $name } } return { $p.name } }",
    )
    .unwrap();
    fs::write(
        cluster.path().join("insert-person.gq"),
        "query insert_person($name: String) { insert Person { name: $name, age: 41 } }",
    )
    .unwrap();
    fs::write(
        cluster.path().join("graph.policy.yaml"),
        "version: 1\ngroups:\n  ops: [\"act-op\"]\nprotected_branches: [main]\nrules:\n  - id: allow-invoke\n    allow:\n      actors: { group: ops }\n      actions: [invoke_query]\n  - id: allow-read\n    allow:\n      actors: { group: ops }\n      actions: [read]\n      branch_scope: any\n  - id: allow-change\n    allow:\n      actors: { group: ops }\n      actions: [change]\n      branch_scope: any\n",
    )
    .unwrap();
    fs::write(
        cluster.path().join("cluster.yaml"),
        "version: 1\nmetadata:\n  name: alias-sys\nstate:\n  backend: cluster\n  lock: true\ngraphs:\n  local:\n    schema: ./local.pg\n    queries:\n      find_person:\n        file: ./find-person.gq\n      insert_person:\n        file: ./insert-person.gq\npolicies:\n  graph:\n    file: ./graph.policy.yaml\n    applies_to: [local]\n",
    )
    .unwrap();
    output_success(cli().arg("cluster").arg("import").arg("--config").arg(cluster.path()));
    output_success(cli().arg("cluster").arg("apply").arg("--config").arg(cluster.path()));
    output_success(
        cli()
            .arg("load")
            .arg("--data")
            .arg(fixture("test.jsonl"))
            .arg("--mode")
            .arg("overwrite")
            .arg(cluster.path().join("graphs").join("local.omni")),
    );
    let server = spawn_server_with_cluster_env(
        cluster.path(),
        &[(
            "OMNIGRAPH_SERVER_BEARER_TOKENS_JSON",
            r#"{"act-op":"srv-tok"}"#,
        )],
    );

    let operator_home = tempfile::tempdir().unwrap();
    fs::write(
        operator_home.path().join("config.yaml"),
        format!(
            "servers:\n  dev:\n    url: {}\naliases:\n  who:\n    server: dev\n    graph: local\n    query: find_person\n    args: [name]\n  create_person:\n    server: dev\n    graph: local\n    query: insert_person\n    args: [name]\n",
            server.base_url
        ),
    )
    .unwrap();
    fs::write(
        operator_home.path().join("credentials"),
        "[dev]\ntoken = srv-tok\n",
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(
            operator_home.path().join("credentials"),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();
    }

    // The operator alias (RFC-011 D4): `alias <name> [args]` — server,
    // graph, stored query, and token all resolve from the operator layer.
    let output = cli()
        .env("OMNIGRAPH_HOME", operator_home.path())
        .arg("alias")
        .arg("who")
        .arg("Alice")
        .arg("--json")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "operator alias must invoke the stored query: {output:?}"
    );
    let payload: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["rows"][0]["p.name"], "Alice", "{payload}");

    // Operator aliases are read-only conveniences: a binding to a stored
    // mutation must be rejected before the server executes it.
    let output = cli()
        .env("OMNIGRAPH_HOME", operator_home.path())
        .arg("alias")
        .arg("create_person")
        .arg("AliasGuardPerson")
        .output()
        .unwrap();
    assert!(!output.status.success(), "mutation alias must fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("'insert_person' is a mutation")
            && stderr.contains("omnigraph mutate insert_person"),
        "expected mutation-kind mismatch; got: {stderr}"
    );
    let output = cli()
        .env("OMNIGRAPH_HOME", operator_home.path())
        .arg("query")
        .arg("find_person")
        .arg("--server")
        .arg("dev")
        .arg("--graph")
        .arg("local")
        .arg("--params")
        .arg(r#"{"name":"AliasGuardPerson"}"#)
        .arg("--json")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "post-alias read should succeed: {output:?}"
    );
    let payload: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        payload["rows"].as_array().unwrap().len(),
        0,
        "mutation alias must not insert AliasGuardPerson: {payload}"
    );

    // --server/--graph: the same stored query via explicit targeting.
    let output = cli()
        .env("OMNIGRAPH_HOME", operator_home.path())
        .arg("query")
        .arg("--server")
        .arg("dev")
        .arg("--graph")
        .arg("local")
        .arg("--query-string")
        .arg("query q($name: String) { match { $p: Person { name: $name } } return { $p.name } }")
        .arg("--params")
        .arg(r#"{"name":"Alice"}"#)
        .arg("--json")
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");

    // RFC-011 D3: invoke the STORED query by name (catalog lane, served-only).
    // No `-e`/`--query` — the positional `find_person` is the catalog name.
    let output = cli()
        .env("OMNIGRAPH_HOME", operator_home.path())
        .arg("query")
        .arg("find_person")
        .arg("--server")
        .arg("dev")
        .arg("--graph")
        .arg("local")
        .arg("--params")
        .arg(r#"{"name":"Alice"}"#)
        .arg("--json")
        .output()
        .unwrap();
    assert!(output.status.success(), "by-name catalog invocation: {output:?}");
    let payload: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["rows"][0]["p.name"], "Alice", "{payload}");

    // The verb asserts kind: `mutate <a-read>` is rejected by the server.
    let output = cli()
        .env("OMNIGRAPH_HOME", operator_home.path())
        .arg("mutate")
        .arg("find_person")
        .arg("--server")
        .arg("dev")
        .arg("--graph")
        .arg("local")
        .arg("--params")
        .arg(r#"{"name":"Alice"}"#)
        .output()
        .unwrap();
    assert!(!output.status.success(), "mutate on a read query must fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("'find_person' is a read — use omnigraph query find_person"),
        "expected a kind-mismatch error; got: {stderr}"
    );

    // Unknown --server errors listing what IS defined.
    let output = cli()
        .env("OMNIGRAPH_HOME", operator_home.path())
        .arg("query")
        .arg("--server")
        .arg("nope")
        .arg("--query-string")
        .arg("query q() { match { $p: Person } return { $p.name } }")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("unknown server 'nope'") && stderr.contains("dev"), "{stderr}");

    // --server is exclusive with --store (two ways to address the graph).
    // (RFC-011 D3: there is no positional URI anymore — the positional is a
    // query name — so the double-addressing contradiction now surfaces between
    // the two scope primitives.)
    let output = cli()
        .env("OMNIGRAPH_HOME", operator_home.path())
        .arg("query")
        .arg("--store")
        .arg(&server.base_url)
        .arg("--server")
        .arg("dev")
        .arg("--query-string")
        .arg("query q() { match { $p: Person } return { $p.name } }")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("exclusive"),
        "{output:?}"
    );
}
