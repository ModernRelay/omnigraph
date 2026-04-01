use omnigraph::db::{Omnigraph, ReadTarget};
use serde_json::Value;
use tempfile::tempdir;

mod support;

use support::*;

#[test]
fn version_command_prints_current_cli_version() {
    let output = output_success(cli().arg("version"));
    let stdout = stdout_string(&output);

    assert_eq!(
        stdout.trim(),
        format!("omnigraph {}", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn short_version_flag_prints_current_cli_version() {
    let output = output_success(cli().arg("-v"));
    let stdout = stdout_string(&output);

    assert_eq!(
        stdout.trim(),
        format!("omnigraph {}", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn long_version_flag_prints_current_cli_version() {
    let output = output_success(cli().arg("--version"));
    let stdout = stdout_string(&output);

    assert_eq!(
        stdout.trim(),
        format!("omnigraph {}", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn init_creates_repo_successfully_on_missing_local_directory() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    let schema = fixture("test.pg");

    let output = output_success(cli().arg("init").arg("--schema").arg(&schema).arg(&repo));
    let stdout = stdout_string(&output);

    assert!(stdout.contains("initialized"));
    assert!(repo.join("_schema.pg").exists());
    assert!(repo.join("_manifest.lance").exists());
    assert!(temp.path().join("omnigraph.yaml").exists());
}

#[test]
fn load_json_outputs_summary_for_main_branch() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    init_repo(&repo);
    let data = fixture("test.jsonl");

    let output = output_success(
        cli()
            .arg("load")
            .arg("--data")
            .arg(&data)
            .arg("--json")
            .arg(&repo),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["branch"], "main");
    assert_eq!(payload["mode"], "overwrite");
    assert_eq!(payload["nodes_loaded"], 2);
    assert_eq!(payload["edges_loaded"], 2);
}

#[test]
fn load_into_feature_branch_with_merge_mode_succeeds() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    init_repo(&repo);
    load_fixture(&repo);

    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--uri")
            .arg(&repo)
            .arg("--from")
            .arg("main")
            .arg("feature"),
    );

    let feature_data = temp.path().join("feature.jsonl");
    write_jsonl(
        &feature_data,
        r#"{"type":"Person","data":{"name":"Alice","age":31}}"#,
    );

    let output = output_success(
        cli()
            .arg("load")
            .arg("--data")
            .arg(&feature_data)
            .arg("--branch")
            .arg("feature")
            .arg("--mode")
            .arg("merge")
            .arg(&repo),
    );
    let stdout = stdout_string(&output);

    assert!(stdout.contains("branch feature"));
    assert!(stdout.contains("with merge"));
    assert!(stdout.contains("1 node types"));
}

#[test]
fn read_json_outputs_rows_for_named_query() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    init_repo(&repo);
    load_fixture(&repo);
    let queries = fixture("test.gq");

    let output = output_success(
        cli()
            .arg("read")
            .arg(&repo)
            .arg("--query")
            .arg(&queries)
            .arg("--name")
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"Alice"}"#)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["query_name"], "get_person");
    assert_eq!(payload["target"]["branch"], "main");
    assert_eq!(payload["row_count"], 1);
    assert_eq!(payload["rows"][0]["p.name"], "Alice");
}

#[test]
fn read_can_resolve_uri_from_config() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    let config = temp.path().join("omnigraph.yaml");
    init_repo(&repo);
    load_fixture(&repo);
    write_config(&config, &local_yaml_config(&repo));

    let output = output_success(
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
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["row_count"], 1);
}

#[test]
fn read_alias_from_yaml_config_runs_with_kv_output() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    let config = temp.path().join("omnigraph.yaml");
    let query = temp.path().join("aliases.gq");
    init_repo(&repo);
    load_fixture(&repo);
    write_query_file(
        &query,
        &std::fs::read_to_string(fixture("test.gq")).unwrap(),
    );
    write_config(
        &config,
        &format!(
            "{}aliases:\n  owner:\n    command: read\n    query: aliases.gq\n    name: get_person\n    args: [name]\n    format: kv\n",
            local_yaml_config(&repo)
        ),
    );

    let output = output_success(
        cli()
            .arg("read")
            .arg("--config")
            .arg(&config)
            .arg("--alias")
            .arg("owner")
            .arg("Alice"),
    );
    let stdout = stdout_string(&output);

    assert!(stdout.contains("row 1"));
    assert!(stdout.contains("p.name: Alice"));
}

#[test]
fn change_alias_from_yaml_config_persists_changes() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    let config = temp.path().join("omnigraph.yaml");
    let query = temp.path().join("mutations.gq");
    init_repo(&repo);
    load_fixture(&repo);
    write_query_file(
        &query,
        r#"
query insert_person($name: String, $age: I32) {
    insert Person { name: $name, age: $age }
}
"#,
    );
    write_config(
        &config,
        &format!(
            "{}aliases:\n  add_person:\n    command: change\n    query: mutations.gq\n    name: insert_person\n    args: [name, age]\n",
            local_yaml_config(&repo)
        ),
    );

    let output = output_success(
        cli()
            .arg("change")
            .arg("--config")
            .arg(&config)
            .arg("--alias")
            .arg("add_person")
            .arg("Eve")
            .arg("29")
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["affected_nodes"], 1);

    let verify = output_success(
        cli()
            .arg("read")
            .arg(&repo)
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("--name")
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"Eve"}"#)
            .arg("--json"),
    );
    let verify_payload: Value = serde_json::from_slice(&verify.stdout).unwrap();
    assert_eq!(verify_payload["row_count"], 1);
}

#[test]
fn read_csv_format_outputs_header_and_row_values() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    init_repo(&repo);
    load_fixture(&repo);

    let output = output_success(
        cli()
            .arg("read")
            .arg(&repo)
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("--name")
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"Alice"}"#)
            .arg("--format")
            .arg("csv"),
    );
    let stdout = stdout_string(&output);

    assert!(stdout.lines().next().unwrap().contains("p.name"));
    assert!(stdout.contains("Alice"));
}

#[test]
fn read_jsonl_format_outputs_metadata_header_first() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    init_repo(&repo);
    load_fixture(&repo);

    let output = output_success(
        cli()
            .arg("read")
            .arg(&repo)
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("--name")
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"Alice"}"#)
            .arg("--format")
            .arg("jsonl"),
    );
    let stdout = stdout_string(&output);
    let mut lines = stdout.lines();
    assert!(lines.next().unwrap().contains("\"kind\":\"metadata\""));
    assert!(lines.next().unwrap().contains("\"p.name\":\"Alice\""));
}

#[test]
fn change_json_outputs_affected_counts_and_persists() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    init_repo(&repo);
    load_fixture(&repo);
    let mutation_file = temp.path().join("mutations.gq");
    write_query_file(
        &mutation_file,
        r#"
query insert_person($name: String, $age: I32) {
    insert Person { name: $name, age: $age }
}
"#,
    );

    let output = output_success(
        cli()
            .arg("change")
            .arg(&repo)
            .arg("--query")
            .arg(&mutation_file)
            .arg("--params")
            .arg(r#"{"name":"Eve","age":29}"#)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["branch"], "main");
    assert_eq!(payload["query_name"], "insert_person");
    assert_eq!(payload["affected_nodes"], 1);
    assert_eq!(payload["affected_edges"], 0);

    let verify = output_success(
        cli()
            .arg("read")
            .arg(&repo)
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("--name")
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"Eve"}"#)
            .arg("--json"),
    );
    let verify_payload: Value = serde_json::from_slice(&verify.stdout).unwrap();
    assert_eq!(verify_payload["row_count"], 1);
    assert_eq!(verify_payload["rows"][0]["p.name"], "Eve");
}

#[test]
fn change_can_resolve_uri_and_branch_from_config() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    let config = temp.path().join("omnigraph.yaml");
    init_repo(&repo);
    load_fixture(&repo);
    write_config(&config, &local_yaml_config(&repo));
    let mutation_file = temp.path().join("config-mutations.gq");
    write_query_file(
        &mutation_file,
        r#"
query insert_person($name: String, $age: I32) {
    insert Person { name: $name, age: $age }
}
"#,
    );

    let output = output_success(
        cli()
            .arg("change")
            .arg("--config")
            .arg(&config)
            .arg("--query")
            .arg(&mutation_file)
            .arg("--params")
            .arg(r#"{"name":"Mia","age":30}"#)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["branch"], "main");
    assert_eq!(payload["affected_nodes"], 1);
}

#[test]
fn read_requires_name_for_multi_query_files() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    init_repo(&repo);
    load_fixture(&repo);

    let output = output_failure(
        cli()
            .arg("read")
            .arg(&repo)
            .arg("--query")
            .arg(fixture("test.gq")),
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("multiple queries"));
}

#[test]
fn branch_create_json_outputs_source_and_name() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    init_repo(&repo);

    let output = output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--uri")
            .arg(&repo)
            .arg("--from")
            .arg("main")
            .arg("feature")
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["from"], "main");
    assert_eq!(payload["name"], "feature");
    assert_eq!(payload["uri"], repo.to_string_lossy().as_ref());
}

#[test]
fn branch_list_outputs_sorted_branches() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    init_repo(&repo);

    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--uri")
            .arg(&repo)
            .arg("--from")
            .arg("main")
            .arg("zeta"),
    );
    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--uri")
            .arg(&repo)
            .arg("--from")
            .arg("main")
            .arg("alpha"),
    );

    let output = output_success(cli().arg("branch").arg("list").arg("--uri").arg(&repo));
    let stdout = stdout_string(&output);
    let lines = stdout
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();

    assert_eq!(lines, vec!["alpha", "main", "zeta"]);
}

#[test]
fn branch_merge_defaults_target_to_main() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    init_repo(&repo);
    load_fixture(&repo);

    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--uri")
            .arg(&repo)
            .arg("--from")
            .arg("main")
            .arg("feature"),
    );

    let feature_data = temp.path().join("feature.jsonl");
    write_jsonl(
        &feature_data,
        r#"{"type":"Person","data":{"name":"Eve","age":29}}"#,
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
            .arg(&repo),
    );

    let merge_output = output_success(
        cli()
            .arg("branch")
            .arg("merge")
            .arg("--uri")
            .arg(&repo)
            .arg("feature")
            .arg("--json"),
    );
    let merge_payload: Value = serde_json::from_slice(&merge_output.stdout).unwrap();
    assert_eq!(merge_payload["source"], "feature");
    assert_eq!(merge_payload["target"], "main");
    assert_eq!(merge_payload["outcome"], "fast_forward");

    let snapshot_output = output_success(
        cli()
            .arg("snapshot")
            .arg(&repo)
            .arg("--branch")
            .arg("main")
            .arg("--json"),
    );
    let snapshot: Value = serde_json::from_slice(&snapshot_output.stdout).unwrap();
    let person_row_count = snapshot["tables"]
        .as_array()
        .unwrap()
        .iter()
        .find(|table| table["table_key"] == "node:Person")
        .unwrap()["row_count"]
        .as_u64()
        .unwrap();
    assert_eq!(person_row_count, 5);
}

#[test]
fn branch_merge_supports_explicit_target() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    init_repo(&repo);
    load_fixture(&repo);

    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--uri")
            .arg(&repo)
            .arg("--from")
            .arg("main")
            .arg("feature"),
    );
    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--uri")
            .arg(&repo)
            .arg("--from")
            .arg("main")
            .arg("experiment"),
    );

    let feature_data = temp.path().join("feature-explicit.jsonl");
    write_jsonl(
        &feature_data,
        r#"{"type":"Person","data":{"name":"Frank","age":41}}"#,
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
            .arg(&repo),
    );

    let merge_output = output_success(
        cli()
            .arg("branch")
            .arg("merge")
            .arg("--uri")
            .arg(&repo)
            .arg("feature")
            .arg("--into")
            .arg("experiment")
            .arg("--json"),
    );
    let merge_payload: Value = serde_json::from_slice(&merge_output.stdout).unwrap();
    assert_eq!(merge_payload["target"], "experiment");
    assert_eq!(merge_payload["outcome"], "fast_forward");
}

#[test]
fn snapshot_json_returns_manifest_version_and_tables() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    init_repo(&repo);
    load_fixture(&repo);

    let output = output_success(cli().arg("snapshot").arg(&repo).arg("--json"));
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["branch"], "main");
    assert!(payload["manifest_version"].as_u64().unwrap() >= 1);
    assert!(payload["tables"].as_array().unwrap().len() >= 4);
}

#[test]
fn snapshot_can_resolve_uri_from_config() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    let config = temp.path().join("omnigraph.yaml");
    init_repo(&repo);
    load_fixture(&repo);
    write_config(&config, &local_yaml_config(&repo));

    let output = output_success(
        cli()
            .arg("snapshot")
            .arg("--config")
            .arg(&config)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["branch"], "main");
}

#[test]
fn snapshot_human_output_includes_branch_and_table_summaries() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    init_repo(&repo);
    load_fixture(&repo);

    let output = output_success(cli().arg("snapshot").arg(&repo));
    let stdout = stdout_string(&output);

    assert!(stdout.contains("branch: main"));
    assert!(stdout.contains("manifest_version:"));
    assert!(stdout.contains("node:Person v"));
    assert!(stdout.contains("edge:Knows v"));
}

#[test]
fn cli_fails_for_missing_repo() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());

    let output = output_failure(cli().arg("snapshot").arg(&repo));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("_schema.pg")
            || stderr.contains("No such file")
            || stderr.contains("not found")
    );
}

#[test]
fn cli_fails_for_missing_schema_or_data_file() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    let missing_schema = temp.path().join("missing.pg");
    let missing_data = temp.path().join("missing.jsonl");

    let init_output = output_failure(
        cli()
            .arg("init")
            .arg("--schema")
            .arg(&missing_schema)
            .arg(&repo),
    );
    assert!(
        String::from_utf8(init_output.stderr)
            .unwrap()
            .contains("No such file")
    );

    init_repo(&repo);
    let load_output = output_failure(
        cli()
            .arg("load")
            .arg("--data")
            .arg(&missing_data)
            .arg(&repo),
    );
    assert!(
        String::from_utf8(load_output.stderr)
            .unwrap()
            .contains("No such file")
    );
}

#[test]
fn cli_fails_for_invalid_merge_requests() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    init_repo(&repo);
    load_fixture(&repo);

    let missing_branch = output_failure(
        cli()
            .arg("branch")
            .arg("merge")
            .arg("--uri")
            .arg(&repo)
            .arg("missing"),
    );
    let missing_branch_stderr = String::from_utf8(missing_branch.stderr).unwrap();
    assert!(
        missing_branch_stderr.contains("missing")
            || missing_branch_stderr.contains("head commit")
            || missing_branch_stderr.contains("not found")
    );

    let same_branch = output_failure(
        cli()
            .arg("branch")
            .arg("merge")
            .arg("--uri")
            .arg(&repo)
            .arg("main")
            .arg("--into")
            .arg("main"),
    );
    assert!(
        String::from_utf8(same_branch.stderr)
            .unwrap()
            .contains("distinct source and target")
    );
}

#[test]
fn run_list_and_show_report_published_runs() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    init_repo(&repo);
    load_fixture(&repo);

    let list_output = output_success(cli().arg("run").arg("list").arg(&repo).arg("--json"));
    let list_payload: Value = serde_json::from_slice(&list_output.stdout).unwrap();
    let runs = list_payload["runs"].as_array().unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0]["status"], "published");
    let run_id = runs[0]["run_id"].as_str().unwrap();

    let show_output = output_success(
        cli()
            .arg("run")
            .arg("show")
            .arg("--uri")
            .arg(&repo)
            .arg(run_id)
            .arg("--json"),
    );
    let show_payload: Value = serde_json::from_slice(&show_output.stdout).unwrap();
    assert_eq!(show_payload["run_id"], run_id);
    assert_eq!(show_payload["status"], "published");
    assert_eq!(show_payload["target_branch"], "main");
}

#[test]
fn run_list_can_resolve_uri_from_config() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    let config = temp.path().join("omnigraph.yaml");
    init_repo(&repo);
    load_fixture(&repo);
    write_config(&config, &local_yaml_config(&repo));

    let output = output_success(
        cli()
            .arg("run")
            .arg("list")
            .arg("--config")
            .arg(&config)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["runs"].as_array().unwrap().len(), 1);
}

#[test]
fn run_publish_promotes_manual_running_run() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    init_repo(&repo);
    load_fixture(&repo);

    let run_id = runtime.block_on(begin_manual_run(&repo, "main"));

    let publish_output = output_success(
        cli()
            .arg("run")
            .arg("publish")
            .arg("--uri")
            .arg(&repo)
            .arg(&run_id)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&publish_output.stdout).unwrap();
    assert_eq!(payload["run_id"], run_id);
    assert_eq!(payload["status"], "published");
    assert!(payload["published_snapshot_id"].is_string());

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let db = Omnigraph::open(repo.to_str().unwrap()).await.unwrap();
        let result = db
            .query(
                ReadTarget::branch("main"),
                include_str!("../../omnigraph/tests/fixtures/test.gq"),
                "get_person",
                &omnigraph_compiler::ir::ParamMap::from([(
                    "name".to_string(),
                    omnigraph_compiler::query::ast::Literal::String("Eve".to_string()),
                )]),
            )
            .await
            .unwrap();
        assert_eq!(result.num_rows(), 1);
    });
}

#[test]
fn run_abort_marks_manual_running_run_aborted() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    init_repo(&repo);
    load_fixture(&repo);

    let run_id = runtime.block_on(begin_manual_run(&repo, "main"));

    let abort_output = output_success(
        cli()
            .arg("run")
            .arg("abort")
            .arg("--uri")
            .arg(&repo)
            .arg(&run_id)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&abort_output.stdout).unwrap();
    assert_eq!(payload["run_id"], run_id);
    assert_eq!(payload["status"], "aborted");
}
