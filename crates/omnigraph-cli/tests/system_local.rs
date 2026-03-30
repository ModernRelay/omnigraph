mod support;

use std::fs;
use std::process::Stdio;
use std::thread::sleep;
use std::time::Duration;

use omnigraph::db::Omnigraph;
use omnigraph::loader::LoadMode;

use support::*;

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
    let payload = parse_stdout_json(&output_success(
        cli().arg("snapshot").arg(repo.path()).arg("--json"),
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
    for _ in 0..200 {
        let payload = parse_stdout_json(&output_success(
            cli().arg("run").arg("list").arg(repo.path()).arg("--json"),
        ));
        if let Some(run_id) = payload["runs"]
            .as_array()
            .unwrap()
            .iter()
            .find(|run| run["target_branch"] == "main" && run["status"] == "running")
            .and_then(|run| run["run_id"].as_str())
        {
            return run_id.to_string();
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
targets:
  local:
    uri: '{}'
cli:
  target: local
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
fn local_cli_transactional_load_drift_fails_without_partial_publish() {
    let repo = SystemRepo::loaded();
    let large_data = repo.write_jsonl("system-large-load.jsonl", &bulk_people_jsonl(80_000));
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
        let mut db = Omnigraph::open(repo.path().to_str().unwrap()).await.unwrap();
        db.load(
            "main",
            r#"{"type":"Person","data":{"name":"Interloper","age":41}}"#,
            LoadMode::Append,
        )
        .await
        .unwrap();
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
