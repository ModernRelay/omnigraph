use std::fs;

use lance_index::traits::DatasetIndexExt;
use omnigraph::db::{Omnigraph, ReadTarget};
use serde_json::Value;
use tempfile::tempdir;

mod support;

use support::*;

const POLICY_YAML: &str = r#"
version: 1
groups:
  team: [act-andrew, act-bruno]
  admins: [act-andrew]
protected_branches: [main]
rules:
  - id: team-read
    allow:
      actors: { group: team }
      actions: [read]
      branch_scope: any
  - id: team-write
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

const POLICY_TESTS_YAML: &str = r#"
version: 1
cases:
  - id: allow-feature-write
    actor: act-andrew
    action: change
    branch: feature
    expect: allow
  - id: deny-main-write
    actor: act-bruno
    action: change
    branch: main
    expect: deny
"#;

fn manifest_dataset_version(repo: &std::path::Path) -> u64 {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        Omnigraph::open(repo.to_string_lossy().as_ref())
            .await
            .unwrap()
            .snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version()
    })
}

fn write_policy_config_fixture(root: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
    let config = root.join("omnigraph.yaml");
    let policy = root.join("policy.yaml");
    fs::write(
        &config,
        r#"
project:
  name: policy-test-repo
policy:
  file: ./policy.yaml
"#,
    )
    .unwrap();
    fs::write(&policy, POLICY_YAML).unwrap();
    fs::write(root.join("policy.tests.yaml"), POLICY_TESTS_YAML).unwrap();
    (config, policy)
}

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
fn embed_seed_fills_missing_and_preserves_existing_vectors_by_default() {
    let temp = tempdir().unwrap();
    let seed = write_seed_fixture(temp.path());

    let output = output_success(
        cli()
            .env("OMNIGRAPH_EMBEDDINGS_MOCK", "1")
            .arg("embed")
            .arg("--seed")
            .arg(&seed)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["mode"], "fill_missing");
    assert_eq!(payload["embedded_rows"], 1);
    assert_eq!(payload["selected_rows"], 2);

    let embedded = read_embedded_rows(temp.path().join("build/seed.embedded.jsonl"));
    assert_eq!(
        embedded[0]["data"]["embedding"].as_array().unwrap().len(),
        4
    );
    assert_eq!(
        embedded[1]["data"]["embedding"],
        serde_json::json!([0.1, 0.2])
    );
}

#[test]
fn embed_clean_removes_selected_embeddings() {
    let temp = tempdir().unwrap();
    let seed = write_seed_fixture(temp.path());

    let output = output_success(
        cli()
            .arg("embed")
            .arg("--seed")
            .arg(&seed)
            .arg("--clean")
            .arg("--select")
            .arg("Decision:slug=dec-beta")
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["mode"], "clean");
    assert_eq!(payload["cleaned_rows"], 1);

    let embedded = read_embedded_rows(temp.path().join("build/seed.embedded.jsonl"));
    assert!(embedded[0]["data"].get("embedding").is_none());
    assert!(embedded[1]["data"].get("embedding").is_none());
}

#[test]
fn embed_select_reembeds_only_matching_rows() {
    let temp = tempdir().unwrap();
    let seed = write_seed_fixture(temp.path());

    let output = output_success(
        cli()
            .env("OMNIGRAPH_EMBEDDINGS_MOCK", "1")
            .arg("embed")
            .arg("--seed")
            .arg(&seed)
            .arg("--select")
            .arg("Decision:slug=dec-beta")
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["mode"], "reembed_selected");
    assert_eq!(payload["embedded_rows"], 1);
    assert_eq!(payload["selected_rows"], 1);

    let embedded = read_embedded_rows(temp.path().join("build/seed.embedded.jsonl"));
    assert!(embedded[0]["data"].get("embedding").is_none());
    assert_ne!(
        embedded[1]["data"]["embedding"],
        serde_json::json!([0.1, 0.2])
    );
    assert_eq!(
        embedded[1]["data"]["embedding"].as_array().unwrap().len(),
        4
    );
}

#[test]
fn embed_seed_preserves_non_entity_rows() {
    let temp = tempdir().unwrap();
    let seed = write_seed_fixture_with_edge(temp.path());

    let output = output_success(
        cli()
            .env("OMNIGRAPH_EMBEDDINGS_MOCK", "1")
            .arg("embed")
            .arg("--seed")
            .arg(&seed)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["rows"], 3);
    assert_eq!(payload["embedded_rows"], 1);

    let embedded = read_embedded_rows(temp.path().join("build/seed.embedded.jsonl"));
    assert_eq!(embedded.len(), 3);
    assert_eq!(embedded[2]["edge"], "Triggered");
    assert_eq!(embedded[2]["from"], "sig-alpha");
    assert_eq!(embedded[2]["to"], "dec-alpha");
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
    assert!(repo.join("__manifest").exists());
    assert!(temp.path().join("omnigraph.yaml").exists());
}

#[test]
fn schema_plan_json_reports_supported_additive_change() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    let schema_path = temp.path().join("next.pg");
    init_repo(&repo);

    let next_schema = fs::read_to_string(fixture("test.pg")).unwrap().replace(
        "    age: I32?\n}",
        "    age: I32?\n    nickname: String?\n}",
    );
    fs::write(&schema_path, next_schema).unwrap();

    let output = output_success(
        cli()
            .arg("schema")
            .arg("plan")
            .arg("--schema")
            .arg(&schema_path)
            .arg("--json")
            .arg(&repo),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["supported"], true);
    assert_eq!(payload["step_count"], 1);
    assert_eq!(payload["steps"][0]["kind"], "add_property");
    assert_eq!(payload["steps"][0]["type_kind"], "node");
    assert_eq!(payload["steps"][0]["type_name"], "Person");
    assert_eq!(payload["steps"][0]["property_name"], "nickname");
}

#[test]
fn schema_plan_json_reports_unsupported_type_change() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    let schema_path = temp.path().join("breaking.pg");
    init_repo(&repo);

    let breaking_schema = fs::read_to_string(fixture("test.pg"))
        .unwrap()
        .replace("age: I32?", "age: I64?");
    fs::write(&schema_path, breaking_schema).unwrap();

    let output = output_success(
        cli()
            .arg("schema")
            .arg("plan")
            .arg("--schema")
            .arg(&schema_path)
            .arg("--json")
            .arg(&repo),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["supported"], false);
    assert!(payload["steps"].as_array().unwrap().iter().any(|step| {
        step["kind"] == "unsupported_change"
            && step["entity"]
                .as_str()
                .unwrap_or_default()
                .contains("Person.age")
    }));
}

#[test]
fn schema_apply_json_applies_supported_migration() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    let schema_path = temp.path().join("next.pg");
    init_repo(&repo);

    let next_schema = fs::read_to_string(fixture("test.pg")).unwrap().replace(
        "    age: I32?\n}",
        "    age: I32?\n    nickname: String?\n}",
    );
    fs::write(&schema_path, next_schema).unwrap();

    let output = output_success(
        cli()
            .arg("schema")
            .arg("apply")
            .arg("--schema")
            .arg(&schema_path)
            .arg("--json")
            .arg(&repo),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["supported"], true);
    assert_eq!(payload["applied"], true);
    assert_eq!(payload["step_count"], 1);

    let db = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(Omnigraph::open(repo.to_string_lossy().as_ref()))
        .unwrap();
    assert!(
        db.catalog().node_types["Person"]
            .properties
            .contains_key("nickname")
    );
}

#[test]
fn schema_apply_human_reports_noop() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    let schema_path = fixture("test.pg");
    init_repo(&repo);

    let output = output_success(
        cli()
            .arg("schema")
            .arg("apply")
            .arg("--schema")
            .arg(&schema_path)
            .arg(&repo),
    );
    let stdout = stdout_string(&output);

    assert!(stdout.contains("applied: no"));
    assert!(stdout.contains("no schema changes"));
}

#[test]
fn schema_apply_json_renames_type_and_updates_snapshot() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    let schema_path = temp.path().join("rename.pg");
    init_repo(&repo);

    let renamed_schema = fs::read_to_string(fixture("test.pg"))
        .unwrap()
        .replace("node Person {\n", "node Human @rename_from(\"Person\") {\n")
        .replace("edge Knows: Person -> Person", "edge Knows: Human -> Human")
        .replace(
            "edge WorksAt: Person -> Company",
            "edge WorksAt: Human -> Company",
        );
    fs::write(&schema_path, renamed_schema).unwrap();

    let output = output_success(
        cli()
            .arg("schema")
            .arg("apply")
            .arg("--schema")
            .arg(&schema_path)
            .arg("--json")
            .arg(&repo),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["applied"], true);

    let db = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(Omnigraph::open(repo.to_string_lossy().as_ref()))
        .unwrap();
    let snapshot = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(db.snapshot_of(ReadTarget::branch("main")))
        .unwrap();
    assert!(snapshot.entry("node:Human").is_some());
    assert!(snapshot.entry("node:Person").is_none());
}

#[test]
fn schema_apply_json_renames_property_and_updates_catalog() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    let schema_path = temp.path().join("rename-property.pg");
    init_repo(&repo);

    let renamed_schema = fs::read_to_string(fixture("test.pg"))
        .unwrap()
        .replace("age: I32?", "years: I32? @rename_from(\"age\")");
    fs::write(&schema_path, renamed_schema).unwrap();

    let output = output_success(
        cli()
            .arg("schema")
            .arg("apply")
            .arg("--schema")
            .arg(&schema_path)
            .arg("--json")
            .arg(&repo),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["applied"], true);

    let db = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(Omnigraph::open(repo.to_string_lossy().as_ref()))
        .unwrap();
    let person = &db.catalog().node_types["Person"];
    assert!(person.properties.contains_key("years"));
    assert!(!person.properties.contains_key("age"));
}

#[test]
fn schema_apply_json_adds_index_for_existing_property() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    let schema_path = temp.path().join("index.pg");
    init_repo(&repo);

    let before_index_count = tokio::runtime::Runtime::new().unwrap().block_on(async {
        let db = Omnigraph::open(repo.to_string_lossy().as_ref())
            .await
            .unwrap();
        let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
        let dataset = snapshot.open("node:Person").await.unwrap();
        dataset.load_indices().await.unwrap().len()
    });

    let indexed_schema = fs::read_to_string(fixture("test.pg"))
        .unwrap()
        .replace("name: String @key", "name: String @key @index");
    fs::write(&schema_path, indexed_schema).unwrap();

    let output = output_success(
        cli()
            .arg("schema")
            .arg("apply")
            .arg("--schema")
            .arg(&schema_path)
            .arg("--json")
            .arg(&repo),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["applied"], true);

    let after_index_count = tokio::runtime::Runtime::new().unwrap().block_on(async {
        let db = Omnigraph::open(repo.to_string_lossy().as_ref())
            .await
            .unwrap();
        let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
        let dataset = snapshot.open("node:Person").await.unwrap();
        dataset.load_indices().await.unwrap().len()
    });
    assert!(after_index_count > before_index_count);
}

#[test]
fn schema_apply_rejects_unsupported_plan() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    let schema_path = temp.path().join("breaking.pg");
    init_repo(&repo);

    let breaking_schema = fs::read_to_string(fixture("test.pg"))
        .unwrap()
        .replace("age: I32?", "age: I64?");
    fs::write(&schema_path, breaking_schema).unwrap();

    let output = output_failure(
        cli()
            .arg("schema")
            .arg("apply")
            .arg("--schema")
            .arg(&schema_path)
            .arg(&repo),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("changing property type"));
}

#[test]
fn schema_apply_rejects_when_non_main_branch_exists() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    let schema_path = temp.path().join("next.pg");
    init_repo(&repo);
    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--from")
            .arg("main")
            .arg("--uri")
            .arg(&repo)
            .arg("feature"),
    );

    let next_schema = fs::read_to_string(fixture("test.pg")).unwrap().replace(
        "    age: I32?\n}",
        "    age: I32?\n    nickname: String?\n}",
    );
    fs::write(&schema_path, next_schema).unwrap();

    let output = output_failure(
        cli()
            .arg("schema")
            .arg("apply")
            .arg("--schema")
            .arg(&schema_path)
            .arg(&repo),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("schema apply requires a repo with only main"));
}

#[test]
fn query_lint_json_with_schema_reports_warnings() {
    let temp = tempdir().unwrap();
    let schema_path = temp.path().join("schema.pg");
    let query_path = temp.path().join("queries.gq");
    write_file(
        &schema_path,
        r#"
node Policy {
    slug: String @key
    name: String?
    effectiveTo: DateTime?
}
"#,
    );
    write_query_file(
        &query_path,
        r#"
query update_policy($slug: String, $name: String) {
    update Policy set { name: $name } where slug = $slug
}
"#,
    );

    let output = output_success(
        cli()
            .arg("query")
            .arg("lint")
            .arg("--query")
            .arg(&query_path)
            .arg("--schema")
            .arg(&schema_path)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["status"], "ok");
    assert_eq!(payload["schema_source"]["kind"], "file");
    assert_eq!(payload["queries_processed"], 1);
    assert_eq!(payload["warnings"], 1);
    assert_eq!(payload["findings"][0]["code"], "L201");
    assert_eq!(
        payload["findings"][0]["message"],
        "Policy.effectiveTo exists in schema but no update query sets it"
    );
}

#[test]
fn query_check_alias_matches_lint_output() {
    let temp = tempdir().unwrap();
    let schema_path = temp.path().join("schema.pg");
    let query_path = temp.path().join("queries.gq");
    write_file(
        &schema_path,
        r#"
node Person {
    name: String
}
"#,
    );
    write_query_file(
        &query_path,
        r#"
query list_people() {
    match { $p: Person }
    return { $p.name }
}
"#,
    );

    let lint_output = output_success(
        cli()
            .arg("query")
            .arg("lint")
            .arg("--query")
            .arg(&query_path)
            .arg("--schema")
            .arg(&schema_path)
            .arg("--json"),
    );
    let check_output = output_success(
        cli()
            .arg("query")
            .arg("check")
            .arg("--query")
            .arg(&query_path)
            .arg("--schema")
            .arg(&schema_path)
            .arg("--json"),
    );

    assert_eq!(stdout_string(&lint_output), stdout_string(&check_output));
}

#[test]
fn query_lint_can_use_local_repo_via_positional_uri() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    let query_path = temp.path().join("queries.gq");
    init_repo(&repo);
    write_query_file(
        &query_path,
        r#"
query list_people() {
    match { $p: Person }
    return { $p.name }
}
"#,
    );

    let output = output_success(
        cli()
            .arg("query")
            .arg("lint")
            .arg("--query")
            .arg(&query_path)
            .arg("--json")
            .arg(&repo),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["status"], "ok");
    assert_eq!(payload["schema_source"]["kind"], "repo");
    assert_eq!(
        payload["schema_source"]["uri"].as_str(),
        Some(repo.to_string_lossy().as_ref())
    );
}

#[test]
fn query_lint_can_resolve_repo_and_query_from_config() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    let config_path = temp.path().join("omnigraph.yaml");
    init_repo(&repo);
    write_query_file(
        &temp.path().join("queries.gq"),
        r#"
query list_people() {
    match { $p: Person }
    return { $p.name }
}
"#,
    );
    write_config(&config_path, &local_yaml_config(&repo));

    let output = output_success(
        cli()
            .arg("query")
            .arg("lint")
            .arg("--query")
            .arg("queries.gq")
            .arg("--config")
            .arg(&config_path)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["status"], "ok");
    assert_eq!(payload["schema_source"]["kind"], "repo");
    assert_eq!(
        payload["schema_source"]["uri"].as_str(),
        Some(repo.to_string_lossy().as_ref())
    );
}

#[test]
fn query_lint_rejects_http_targets_without_schema() {
    let temp = tempdir().unwrap();
    let query_path = temp.path().join("queries.gq");
    write_query_file(
        &query_path,
        r#"
query list_people() {
    match { $p: Person }
    return { $p.name }
}
"#,
    );

    let output = output_failure(
        cli()
            .arg("query")
            .arg("lint")
            .arg("--query")
            .arg(&query_path)
            .arg("http://127.0.0.1:8080"),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("query lint is only supported against local repo URIs in this milestone")
    );
}

#[test]
fn query_lint_requires_schema_or_resolvable_repo_target() {
    let temp = tempdir().unwrap();
    let query_path = temp.path().join("queries.gq");
    write_query_file(
        &query_path,
        r#"
query list_people() {
    match { $p: Person }
    return { $p.name }
}
"#,
    );

    let output = output_failure(
        cli()
            .arg("query")
            .arg("lint")
            .arg("--query")
            .arg(&query_path),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("query lint requires --schema <schema.pg> or a resolvable repo target")
    );
}

#[test]
fn query_lint_human_output_reports_warnings() {
    let temp = tempdir().unwrap();
    let schema_path = temp.path().join("schema.pg");
    let query_path = temp.path().join("queries.gq");
    write_file(
        &schema_path,
        r#"
node Policy {
    slug: String @key
    name: String?
    effectiveTo: DateTime?
}
"#,
    );
    write_query_file(
        &query_path,
        r#"
query update_policy($slug: String, $name: String) {
    update Policy set { name: $name } where slug = $slug
}
"#,
    );

    let output = output_success(
        cli()
            .arg("query")
            .arg("lint")
            .arg("--query")
            .arg(&query_path)
            .arg("--schema")
            .arg(&schema_path),
    );
    let stdout = stdout_string(&output);

    assert!(stdout.contains("OK    query `update_policy` (mutation)"));
    assert!(
        stdout.contains("WARN  Policy.effectiveTo exists in schema but no update query sets it")
    );
    assert!(stdout.contains(
        "INFO  Lint complete: 1 queries processed (0 error(s), 1 warning(s), 0 info item(s))"
    ));
}

#[test]
fn query_lint_human_output_reports_strict_validation_errors() {
    let temp = tempdir().unwrap();
    let schema_path = temp.path().join("schema.pg");
    let query_path = temp.path().join("queries.gq");
    write_file(
        &schema_path,
        r#"
node Policy {
    slug: String @key
    name: String?
}
"#,
    );
    write_query_file(
        &query_path,
        r#"
query bad_update($slug: String) {
    update Policy set { priority_level: "high" } where slug = $slug
}
"#,
    );

    let output = output_failure(
        cli()
            .arg("query")
            .arg("lint")
            .arg("--query")
            .arg(&query_path)
            .arg("--schema")
            .arg(&schema_path),
    );
    let stdout = stdout_string(&output);

    assert!(stdout.contains("ERROR query `bad_update`:"));
    assert!(stdout.contains("Policy"));
    assert!(stdout.contains(
        "INFO  Lint complete: 1 queries processed (1 error(s), 0 warning(s), 0 info item(s))"
    ));
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
    assert_eq!(payload["nodes_loaded"], 6);
    assert_eq!(payload["edges_loaded"], 5);
    assert_eq!(payload["node_types_loaded"], 2);
    assert_eq!(payload["edge_types_loaded"], 2);
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
    assert!(stdout.contains("1 nodes across 1 node types"));
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
fn export_jsonl_outputs_source_rows_for_selected_branch_and_type() {
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

    let feature_data = temp.path().join("feature-export.jsonl");
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

    let output = output_success(
        cli()
            .arg("export")
            .arg(&repo)
            .arg("--branch")
            .arg("feature")
            .arg("--type")
            .arg("Person")
            .arg("--jsonl"),
    );
    let rows = stdout_string(&output)
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();

    assert_eq!(rows.len(), 5);
    assert!(rows.iter().all(|row| row["type"] == "Person"));
    assert!(rows.iter().all(|row| row.get("edge").is_none()));
    assert!(
        rows.iter()
            .any(|row| row["data"]["name"].as_str() == Some("Eve"))
    );
}

#[test]
fn policy_validate_accepts_valid_policy_file() {
    let temp = tempdir().unwrap();
    let (config, _) = write_policy_config_fixture(temp.path());

    let output = output_success(
        cli()
            .arg("policy")
            .arg("validate")
            .arg("--config")
            .arg(&config),
    );
    let stdout = stdout_string(&output);

    assert!(stdout.contains("policy valid:"));
    assert!(stdout.contains("policy.yaml"));
    assert!(stdout.contains("[2 actors]"));
}

#[test]
fn policy_validate_fails_for_invalid_policy_file() {
    let temp = tempdir().unwrap();
    let config = temp.path().join("omnigraph.yaml");
    let policy = temp.path().join("policy.yaml");
    fs::write(
        &config,
        r#"
project:
  name: policy-test-repo
policy:
  file: ./policy.yaml
"#,
    )
    .unwrap();
    fs::write(
        &policy,
        r#"
version: 1
groups:
  team: [act-andrew]
rules:
  - id: duplicate
    allow:
      actors: { group: team }
      actions: [read]
      branch_scope: any
  - id: duplicate
    allow:
      actors: { group: team }
      actions: [export]
      branch_scope: any
"#,
    )
    .unwrap();

    let output = output_failure(
        cli()
            .arg("policy")
            .arg("validate")
            .arg("--config")
            .arg(&config),
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("duplicate policy rule id"));
}

#[test]
fn policy_test_runs_declarative_cases() {
    let temp = tempdir().unwrap();
    let (config, _) = write_policy_config_fixture(temp.path());

    let output = output_success(cli().arg("policy").arg("test").arg("--config").arg(&config));
    let stdout = stdout_string(&output);

    assert!(stdout.contains("policy tests passed: 2 cases"));
}

#[test]
fn policy_explain_reports_decision_and_matched_rule() {
    let temp = tempdir().unwrap();
    let (config, _) = write_policy_config_fixture(temp.path());

    let allow = output_success(
        cli()
            .arg("policy")
            .arg("explain")
            .arg("--config")
            .arg(&config)
            .arg("--actor")
            .arg("act-andrew")
            .arg("--action")
            .arg("change")
            .arg("--branch")
            .arg("feature"),
    );
    let allow_stdout = stdout_string(&allow);
    assert!(allow_stdout.contains("decision: allow"));
    assert!(allow_stdout.contains("matched_rule: team-write"));

    let deny = output_success(
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
    let deny_stdout = stdout_string(&deny);
    assert!(deny_stdout.contains("decision: deny"));
    assert!(deny_stdout.contains("message: policy denied action 'change' on branch 'main'"));
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
fn read_alias_uses_alias_target_without_cli_default_and_accepts_url_like_arg() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    let config = temp.path().join("omnigraph.yaml");
    let query = temp.path().join("aliases.gq");
    let data = temp.path().join("url-like.jsonl");
    init_repo(&repo);
    write_jsonl(
        &data,
        r#"{"type":"Person","data":{"name":"https://example.com","age":30}}"#,
    );
    output_success(cli().arg("load").arg("--data").arg(&data).arg(&repo));
    write_query_file(
        &query,
        &std::fs::read_to_string(fixture("test.gq")).unwrap(),
    );
    write_config(
        &config,
        &format!(
            "graphs:\n  local:\n    uri: '{}'\nquery:\n  roots:\n    - .\npolicy: {{}}\naliases:\n  owner:\n    command: read\n    query: aliases.gq\n    name: get_person\n    args: [name]\n    graph: local\n    format: kv\n",
            repo.to_string_lossy()
        ),
    );

    let output = output_success(
        cli()
            .arg("read")
            .arg("--config")
            .arg(&config)
            .arg("--alias")
            .arg("owner")
            .arg("https://example.com"),
    );
    let stdout = stdout_string(&output);

    assert!(stdout.contains("row 1"));
    assert!(stdout.contains("p.name: https://example.com"));
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
fn branch_delete_json_outputs_name_and_removes_branch() {
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
            .arg("feature"),
    );

    let output = output_success(
        cli()
            .arg("branch")
            .arg("delete")
            .arg("--uri")
            .arg(&repo)
            .arg("feature")
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["name"], "feature");
    assert_eq!(payload["uri"], repo.to_string_lossy().as_ref());

    let listed = output_success(cli().arg("branch").arg("list").arg("--uri").arg(&repo));
    let stdout = stdout_string(&listed);
    let lines = stdout
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    assert_eq!(lines, vec!["main"]);
}

#[test]
fn branch_delete_rejects_main() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    init_repo(&repo);

    let output = output_failure(
        cli()
            .arg("branch")
            .arg("delete")
            .arg("--uri")
            .arg(&repo)
            .arg("main"),
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("cannot delete branch 'main'"));
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
    assert_eq!(
        payload["manifest_version"].as_u64().unwrap(),
        manifest_dataset_version(&repo)
    );
    assert!(payload["tables"].as_array().unwrap().len() >= 4);
}

fn write_seed_fixture(root: &std::path::Path) -> std::path::PathBuf {
    fs::create_dir_all(root.join("data")).unwrap();
    fs::create_dir_all(root.join("build")).unwrap();
    let raw_seed = root.join("data/seed.jsonl");
    let seed = root.join("seed.yaml");

    fs::write(
        &raw_seed,
        concat!(
            "{\"type\":\"Decision\",\"data\":{\"slug\":\"dec-alpha\",\"intent\":\"Alpha ship\"}}\n",
            "{\"type\":\"Decision\",\"data\":{\"slug\":\"dec-beta\",\"intent\":\"Beta ship\",\"embedding\":[0.1,0.2]}}\n"
        ),
    )
    .unwrap();

    fs::write(
        &seed,
        concat!(
            "graph:\n",
            "  slug: mr-context-graph\n",
            "sources:\n",
            "  raw_seed: ./data/seed.jsonl\n",
            "artifacts:\n",
            "  embedded_seed: ./build/seed.embedded.jsonl\n",
            "embeddings:\n",
            "  model: gemini-embedding-2-preview\n",
            "  dimension: 4\n",
            "  types:\n",
            "    Decision:\n",
            "      target: embedding\n",
            "      fields: [slug, intent]\n"
        ),
    )
    .unwrap();

    seed
}

fn write_seed_fixture_with_edge(root: &std::path::Path) -> std::path::PathBuf {
    let seed = write_seed_fixture(root);
    let raw_seed = root.join("data/seed.jsonl");
    fs::write(
        &raw_seed,
        concat!(
            "{\"type\":\"Decision\",\"data\":{\"slug\":\"dec-alpha\",\"intent\":\"Alpha ship\"}}\n",
            "{\"type\":\"Decision\",\"data\":{\"slug\":\"dec-beta\",\"intent\":\"Beta ship\",\"embedding\":[0.1,0.2]}}\n",
            "{\"edge\":\"Triggered\",\"from\":\"sig-alpha\",\"to\":\"dec-alpha\"}\n"
        ),
    )
    .unwrap();
    seed
}

fn read_embedded_rows(path: std::path::PathBuf) -> Vec<Value> {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
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
fn commit_show_accepts_long_uri_flag() {
    let temp = tempdir().unwrap();
    let repo = repo_path(temp.path());
    init_repo(&repo);
    load_fixture(&repo);

    let list = output_success(cli().arg("commit").arg("list").arg(&repo).arg("--json"));
    let list_payload: Value = serde_json::from_slice(&list.stdout).unwrap();
    let commit_id = list_payload["commits"][0]["graph_commit_id"]
        .as_str()
        .unwrap()
        .to_string();

    let output = output_success(
        cli()
            .arg("commit")
            .arg("show")
            .arg("--uri")
            .arg(&repo)
            .arg(&commit_id)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["graph_commit_id"], commit_id);
    assert!(payload["manifest_version"].as_u64().unwrap() >= 1);
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

// MR-771: `omnigraph run list/show/publish/abort` subcommands removed
// alongside the run state machine. Direct-to-target writes leave nothing
// for these CLIs to manage. Audit history is now visible via
// `omnigraph commit list` reading the commit graph.
