mod support;

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
