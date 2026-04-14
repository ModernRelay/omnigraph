mod support;

use std::env;
use std::fs;
use std::process::Stdio;
use std::thread::sleep;
use std::time::Duration;

use omnigraph::db::Omnigraph;
use omnigraph::loader::LoadMode;
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
      actions: [branch_merge, run_publish]
      target_branch_scope: protected
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

fn local_policy_config(repo: &SystemRepo) -> String {
    format!(
        "\
project:
  name: policy-e2e-local
graphs:
  local:
    uri: {}
cli:
  graph: local
  branch: main
query:
  roots:
    - .
policy:
  file: ./policy.yaml
",
        yaml_string(&repo.path().to_string_lossy())
    )
}

fn insert_person_query(repo: &SystemRepo, name: &str) -> std::path::PathBuf {
    repo.write_query(
        name,
        r#"
query insert_person($name: String, $age: I32) {
    insert Person { name: $name, age: $age }
}
"#,
    )
}

fn add_friend_query(repo: &SystemRepo, name: &str) -> std::path::PathBuf {
    repo.write_query(
        name,
        r#"
query add_friend($from: String, $to: String) {
    insert Knows { from: $from, to: $to }
}
"#,
    )
}

fn snapshot_table_row_count(repo: &SystemRepo, table_key: &str) -> u64 {
    snapshot_table_row_count_at(repo.path(), table_key)
}

fn snapshot_table_row_count_at(repo: &std::path::Path, table_key: &str) -> u64 {
    let payload = parse_stdout_json(&output_success(
        cli().arg("snapshot").arg(repo).arg("--json"),
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

fn wait_for_running_run(repo: &SystemRepo) -> String {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    for _ in 0..200 {
        let running = runtime.block_on(async {
            let db = Omnigraph::open(repo.path().to_str().unwrap())
                .await
                .unwrap();
            db.list_runs()
                .await
                .unwrap()
                .into_iter()
                .find(|run| run.target_branch == "main" && run.status.as_str() == "running")
                .map(|run| run.run_id.to_string())
        });
        if let Some(run_id) = running {
            return run_id;
        }
        sleep(Duration::from_millis(50));
    }

    panic!("timed out waiting for running run");
}

fn bulk_people_jsonl(count: usize) -> String {
    let mut rows = String::new();
    for index in 0..count {
        rows.push_str(&format!(
            r#"{{"type":"Person","data":{{"name":"Bulk{:05}","age":{}}}}}"#,
            index,
            20 + (index % 50)
        ));
        rows.push('\n');
    }
    rows
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

fn s3_test_repo_uri(suite: &str) -> Option<String> {
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
    let repo = SystemRepo::initialized();
    let mutation_file = insert_person_query(&repo, "system-local-init-change.gq");

    output_success(
        cli()
            .arg("load")
            .arg("--data")
            .arg(fixture("test.jsonl"))
            .arg(repo.path()),
    );

    let read_before = parse_stdout_json(&output_success(
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
    assert_eq!(read_before["row_count"], 1);
    assert_eq!(read_before["rows"][0]["p.name"], "Alice");

    let change_payload = parse_stdout_json(&output_success(
        cli()
            .arg("change")
            .arg(repo.path())
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
            .arg(repo.path())
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
}

#[test]
fn local_cli_end_to_end_branch_change_merge_flow() {
    let repo = SystemRepo::loaded();
    let mutation_file = insert_person_query(&repo, "system-local-change.gq");

    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--uri")
            .arg(repo.path())
            .arg("--from")
            .arg("main")
            .arg("feature"),
    );

    let change_payload = parse_stdout_json(&output_success(
        cli()
            .arg("change")
            .arg(repo.path())
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
            .arg(repo.path())
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
            .arg(repo.path())
            .arg("feature")
            .arg("--json"),
    ));
    assert_eq!(merge_payload["target"], "main");

    let main_read = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg(repo.path())
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

    let runs_payload = parse_stdout_json(&output_success(
        cli().arg("run").arg("list").arg(repo.path()).arg("--json"),
    ));
    let runs = runs_payload["runs"].as_array().unwrap();
    assert!(runs.len() >= 2);
    assert!(
        runs.iter()
            .any(|run| run["target_branch"] == "feature" && run["status"] == "published")
    );
}

#[test]
fn local_cli_ingest_creates_review_branch_and_keeps_it_readable() {
    let repo = SystemRepo::loaded();
    let ingest_data = repo.write_jsonl(
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
            .arg(repo.path())
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
            .arg(repo.path())
            .arg("--branch")
            .arg("feature-ingest")
            .arg("--json"),
    ));
    assert_eq!(feature_snapshot["branch"], "feature-ingest");

    let zoe = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg(repo.path())
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
            .arg(repo.path())
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
    let repo = SystemRepo::loaded();

    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--uri")
            .arg(repo.path())
            .arg("--from")
            .arg("main")
            .arg("feature"),
    );

    let feature_data = repo.write_jsonl(
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
            .arg(repo.path()),
    );

    let exported = stdout_string(&output_success(
        cli()
            .arg("export")
            .arg(repo.path())
            .arg("--branch")
            .arg("feature")
            .arg("--jsonl"),
    ));
    let export_path = repo.write_jsonl("system-local-exported.jsonl", &exported);
    let imported_repo = repo.path().parent().unwrap().join("imported-export.omni");

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

    assert_eq!(
        snapshot_table_row_count_at(&imported_repo, "node:Person"),
        5
    );
    assert_eq!(
        snapshot_table_row_count_at(&imported_repo, "node:Company"),
        2
    );
    assert_eq!(snapshot_table_row_count_at(&imported_repo, "edge:Knows"), 4);
    assert_eq!(
        snapshot_table_row_count_at(&imported_repo, "edge:WorksAt"),
        2
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

    let friends = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg(&imported_repo)
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
    let Some(repo_uri) = s3_test_repo_uri("cli-local") else {
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
            repo_uri
        ),
    );

    output_success(
        cli()
            .arg("init")
            .arg("--schema")
            .arg(fixture("test.pg"))
            .arg(&repo_uri),
    );
    output_success(
        cli()
            .arg("load")
            .arg("--data")
            .arg(fixture("test.jsonl"))
            .arg(&repo_uri),
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
    let repo = SystemRepo::loaded();
    let bad_data = repo.write_jsonl(
        "system-bad-load.jsonl",
        r#"{"edge":"Knows","from":"Alice","to":"Missing"}"#,
    );
    let person_rows_before = snapshot_table_row_count(&repo, "node:Person");
    let knows_rows_before = snapshot_table_row_count(&repo, "edge:Knows");

    let output = output_failure(
        cli()
            .arg("load")
            .arg("--data")
            .arg(&bad_data)
            .arg("--mode")
            .arg("append")
            .arg(repo.path()),
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("not found") || stderr.contains("Missing"));

    assert_eq!(
        snapshot_table_row_count(&repo, "node:Person"),
        person_rows_before
    );
    assert_eq!(
        snapshot_table_row_count(&repo, "edge:Knows"),
        knows_rows_before
    );

    let runs_payload = parse_stdout_json(&output_success(
        cli().arg("run").arg("list").arg(repo.path()).arg("--json"),
    ));
    assert!(
        runs_payload["runs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|run| run["target_branch"] == "main" && run["status"] == "failed")
    );
}

#[test]
fn local_cli_failed_change_keeps_target_state_unchanged() {
    let repo = SystemRepo::loaded();
    let mutation_file = add_friend_query(&repo, "system-invalid-change.gq");

    let output = output_failure(
        cli()
            .arg("change")
            .arg(repo.path())
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
            .arg(repo.path())
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("--name")
            .arg("friends_of")
            .arg("--params")
            .arg(r#"{"name":"Alice"}"#)
            .arg("--json"),
    ));
    assert_eq!(friends_payload["row_count"], 2);

    let runs_payload = parse_stdout_json(&output_success(
        cli().arg("run").arg("list").arg(repo.path()).arg("--json"),
    ));
    assert!(
        runs_payload["runs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|run| run["target_branch"] == "main" && run["status"] == "failed")
    );
}

#[test]
fn local_cli_resolves_relative_query_against_config_base_dir() {
    let repo = SystemRepo::loaded();
    let root = repo.path().parent().unwrap();
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
            repo.path().display()
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
    let repo = repo_path(temp.path());
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

    output_success(cli().arg("init").arg("--schema").arg(&schema).arg(&repo));
    output_success(cli().arg("load").arg("--data").arg(&data).arg(&repo));

    let filtered = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg(&repo)
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
            .arg(&repo)
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
            .arg(&repo)
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
            .arg(&repo)
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
    let repo = repo_path(temp.path());
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

    output_success(cli().arg("init").arg("--schema").arg(&schema).arg(&repo));
    output_success(cli().arg("load").arg("--data").arg(&data).arg(&repo));

    let result = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg(&repo)
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

#[test]
fn local_cli_transactional_load_drift_fails_without_partial_publish() {
    let repo = SystemRepo::loaded();
    let large_data = repo.write_jsonl("system-large-load.jsonl", &bulk_people_jsonl(250_000));
    let person_rows_before = snapshot_table_row_count(&repo, "node:Person");

    let mut load = cli_process();
    load.arg("load")
        .arg("--data")
        .arg(&large_data)
        .arg("--mode")
        .arg("merge")
        .arg(repo.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let child = load.spawn().unwrap();

    let run_id = wait_for_running_run(&repo);

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let mut db = Omnigraph::open(repo.path().to_str().unwrap())
            .await
            .unwrap();
        let interloper = db
            .begin_run("main", Some("system-test-interloper"))
            .await
            .unwrap();
        db.load(
            interloper.run_branch.as_str(),
            r#"{"type":"Person","data":{"name":"Interloper","age":41}}"#,
            LoadMode::Append,
        )
        .await
        .unwrap();
        db.publish_run(&interloper.run_id).await.unwrap();
    });

    let output = child.wait_with_output().unwrap();
    assert!(
        !output.status.success(),
        "load unexpectedly succeeded\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("advanced during transactional load")
            || stderr.contains("version drift")
            || stderr.contains("retry"),
        "unexpected load failure: {stderr}"
    );

    let run_payload = parse_stdout_json(&output_success(
        cli()
            .arg("run")
            .arg("show")
            .arg("--uri")
            .arg(repo.path())
            .arg(&run_id)
            .arg("--json"),
    ));
    assert_eq!(run_payload["status"], "failed");

    assert_eq!(
        snapshot_table_row_count(&repo, "node:Person"),
        person_rows_before + 1
    );

    let interloper = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg(repo.path())
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("--name")
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"Interloper"}"#)
            .arg("--json"),
    ));
    assert_eq!(interloper["row_count"], 1);

    let bulk_row = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg(repo.path())
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("--name")
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"Bulk00000"}"#)
            .arg("--json"),
    ));
    assert_eq!(bulk_row["row_count"], 0);
}

#[test]
fn local_cli_policy_tooling_is_end_to_end_while_local_writes_stay_unenforced() {
    let repo = SystemRepo::loaded();
    let config = repo.write_config("omnigraph-policy.yaml", &local_policy_config(&repo));
    repo.write_config("policy.yaml", POLICY_E2E_YAML);
    repo.write_config("policy.tests.yaml", POLICY_E2E_TESTS_YAML);
    let mutation_file = insert_person_query(&repo, "system-local-policy-change.gq");

    let validate = output_success(
        cli()
            .arg("policy")
            .arg("validate")
            .arg("--config")
            .arg(&config),
    );
    assert!(stdout_string(&validate).contains("policy valid:"));

    let tests = output_success(cli().arg("policy").arg("test").arg("--config").arg(&config));
    assert!(stdout_string(&tests).contains("policy tests passed: 2 cases"));

    let explain = output_success(
        cli()
            .arg("policy")
            .arg("explain")
            .arg("--config")
            .arg(&config)
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

    let local_change = parse_stdout_json(&output_success(
        cli()
            .arg("change")
            .arg("--config")
            .arg(&config)
            .arg("--query")
            .arg(&mutation_file)
            .arg("--params")
            .arg(r#"{"name":"PolicyLocal","age":44}"#)
            .arg("--json"),
    ));
    assert_eq!(local_change["branch"], "main");
    assert_eq!(local_change["affected_nodes"], 1);

    let verify = parse_stdout_json(&output_success(
        cli()
            .arg("read")
            .arg(repo.path())
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("--name")
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"PolicyLocal"}"#)
            .arg("--json"),
    ));
    assert_eq!(verify["row_count"], 1);
    assert_eq!(verify["rows"][0]["p.name"], "PolicyLocal");
}
