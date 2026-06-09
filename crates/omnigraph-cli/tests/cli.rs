use std::fs;

use lance::Dataset;
use lance::index::DatasetIndexExt;
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

fn manifest_dataset_version(graph: &std::path::Path) -> u64 {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        Omnigraph::open(graph.to_string_lossy().as_ref())
            .await
            .unwrap()
            .snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version()
    })
}

fn forge_person_delete_drift(graph: &std::path::Path) -> (u64, u64) {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let uri = graph.to_string_lossy();
        let db = Omnigraph::open(uri.as_ref()).await.unwrap();
        let snap = db
            .snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap();
        let entry = snap.entry("node:Person").unwrap();
        let full_path = format!("{}/{}", uri.trim_end_matches('/'), entry.table_path);
        let mut ds = Dataset::open(&full_path).await.unwrap();
        let deleted = ds.delete("name = 'Alice'").await.unwrap();
        assert_eq!(deleted.num_deleted_rows, 1);
        let head = deleted.new_dataset.version().version;
        assert!(head > entry.table_version);
        (entry.table_version, head)
    })
}

fn write_policy_config_fixture(root: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
    let config = root.join("omnigraph.yaml");
    let policy = root.join("policy.yaml");
    fs::write(
        &config,
        r#"
project:
  name: policy-test-graph
policy:
  file: ./policy.yaml
"#,
    )
    .unwrap();
    fs::write(&policy, POLICY_YAML).unwrap();
    fs::write(root.join("policy.tests.yaml"), POLICY_TESTS_YAML).unwrap();
    (config, policy)
}

fn write_cluster_config_fixture(root: &std::path::Path) {
    fs::write(
        root.join("people.pg"),
        r#"
node Person {
  name: String @key
  age: I32?
}
"#,
    )
    .unwrap();
    fs::write(
        root.join("people.gq"),
        r#"
query find_person($name: String) {
  match { $p: Person { name: $name } }
  return { $p.name, $p.age }
}
"#,
    )
    .unwrap();
    fs::write(root.join("base.policy.yaml"), "rules: []\n").unwrap();
    fs::write(
        root.join("cluster.yaml"),
        r#"
version: 1
metadata:
  name: company-brain
state:
  backend: cluster
  lock: true
graphs:
  knowledge:
    schema: ./people.pg
    queries:
      find_person:
        file: ./people.gq
policies:
  base:
    file: ./base.policy.yaml
    applies_to: [knowledge]
"#,
    )
    .unwrap();
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
fn cluster_validate_config_success() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());

    let output = output_success(
        cli()
            .arg("cluster")
            .arg("validate")
            .arg("--config")
            .arg(temp.path()),
    );
    let stdout = stdout_string(&output);
    assert!(stdout.contains("cluster config valid"), "{stdout}");
}

#[test]
fn cluster_validate_json_is_stable() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());

    let json = parse_stdout_json(&output_success(
        cli()
            .arg("cluster")
            .arg("validate")
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(json["ok"], true);
    assert!(json["resource_digests"]["graph.knowledge"].is_string());
    assert!(json["resource_digests"]["query.knowledge.find_person"].is_string());
    assert_eq!(json["dependencies"][0]["from"], "policy.base");
    assert_eq!(json["dependencies"][0]["to"], "graph.knowledge");
}

#[test]
fn cluster_plan_json_reads_inferred_local_state() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    let state_dir = temp.path().join("__cluster");
    fs::create_dir_all(&state_dir).unwrap();
    fs::write(
        state_dir.join("state.json"),
        r#"
{
  "version": 1,
  "applied_revision": {
    "config_digest": "old",
    "resources": {
      "graph.knowledge": { "digest": "old-graph" },
      "policy.old": { "digest": "old-policy" }
    }
  }
}
"#,
    )
    .unwrap();

    let json = parse_stdout_json(&output_success(
        cli()
            .arg("cluster")
            .arg("plan")
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(json["ok"], true);
    assert_eq!(json["state_observations"]["state_found"], true);
    assert!(
        json["changes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|change| change["resource"] == "policy.old" && change["operation"] == "delete"),
        "plan should read state and delete stale resources: {json}"
    );
}

#[test]
fn cluster_status_json_reports_missing_state() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());

    let json = parse_stdout_json(&output_success(
        cli()
            .arg("cluster")
            .arg("status")
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(json["ok"], true);
    assert_eq!(json["state_observations"]["state_found"], false);
    assert!(
        json["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|diagnostic| diagnostic["code"] == "state_missing"),
        "missing state should be a warning diagnostic: {json}"
    );
}

#[test]
fn cluster_status_json_reports_extended_state() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    let state_dir = temp.path().join("__cluster");
    fs::create_dir_all(&state_dir).unwrap();
    fs::write(
        state_dir.join("state.json"),
        r#"
{
  "version": 1,
  "state_revision": 5,
  "applied_revision": {
    "config_digest": "applied",
    "resources": {
      "graph.knowledge": { "digest": "graph-digest" }
    }
  },
  "resource_statuses": {
    "graph.knowledge": { "status": "applied", "conditions": ["healthy"] }
  },
  "approval_records": {},
  "recovery_records": {},
  "observations": {}
}
"#,
    )
    .unwrap();

    let json = parse_stdout_json(&output_success(
        cli()
            .arg("cluster")
            .arg("status")
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(json["ok"], true);
    assert_eq!(json["state_observations"]["state_revision"], 5);
    assert!(
        json["state_observations"]["state_cas"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    assert_eq!(json["resource_digests"]["graph.knowledge"], "graph-digest");
    assert_eq!(
        json["resource_statuses"]["graph.knowledge"]["status"],
        "applied"
    );
}

#[test]
fn cluster_plan_json_includes_state_cas_revision_and_lock_observation() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    let state_dir = temp.path().join("__cluster");
    fs::create_dir_all(&state_dir).unwrap();
    fs::write(
        state_dir.join("state.json"),
        r#"
{
  "version": 1,
  "state_revision": 9,
  "applied_revision": {
    "config_digest": "old",
    "resources": {
      "graph.knowledge": { "digest": "old-graph" }
    }
  }
}
"#,
    )
    .unwrap();

    let json = parse_stdout_json(&output_success(
        cli()
            .arg("cluster")
            .arg("plan")
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    ));
    assert_eq!(json["ok"], true);
    assert_eq!(json["state_observations"]["state_revision"], 9);
    assert!(
        json["state_observations"]["state_cas"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    assert_eq!(json["state_observations"]["locked"], false);
    assert_eq!(json["state_observations"]["lock_acquired"], true);
    assert!(json["state_observations"]["acquired_lock_id"].is_string());
    assert!(!state_dir.join("lock.json").exists());
}

#[test]
fn cluster_plan_locked_state_exits_nonzero() {
    let temp = tempdir().unwrap();
    write_cluster_config_fixture(temp.path());
    let state_dir = temp.path().join("__cluster");
    fs::create_dir_all(&state_dir).unwrap();
    fs::write(
        state_dir.join("lock.json"),
        r#"
{
  "version": 1,
  "lock_id": "held-lock",
  "operation": "plan",
  "created_at": "2026-06-08T00:00:00Z",
  "pid": 123
}
"#,
    )
    .unwrap();

    let output = output_failure(
        cli()
            .arg("cluster")
            .arg("plan")
            .arg("--config")
            .arg(temp.path())
            .arg("--json"),
    );
    let json = parse_stdout_json(&output);
    assert_eq!(json["ok"], false);
    assert_eq!(json["state_observations"]["locked"], true);
    assert_eq!(json["state_observations"]["lock_acquired"], false);
    assert_eq!(json["state_observations"]["lock_id"], "held-lock");
    assert!(
        json["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|diagnostic| diagnostic["code"] == "state_lock_held"),
        "locked state should produce a useful diagnostic: {json}"
    );
}

#[test]
fn cluster_validate_invalid_config_exits_nonzero() {
    let temp = tempdir().unwrap();
    fs::write(
        temp.path().join("cluster.yaml"),
        "version: 1\ngraphs: {}\npipelines: {}\n",
    )
    .unwrap();

    let output = output_failure(
        cli()
            .arg("cluster")
            .arg("validate")
            .arg("--config")
            .arg(temp.path()),
    );
    let stdout = stdout_string(&output);
    assert!(stdout.contains("future_phase_field"), "{stdout}");
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
fn init_creates_graph_successfully_on_missing_local_directory() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    let schema = fixture("test.pg");

    let output = output_success(cli().arg("init").arg("--schema").arg(&schema).arg(&graph));
    let stdout = stdout_string(&output);

    assert!(stdout.contains("initialized"));
    assert!(graph.join("_schema.pg").exists());
    assert!(graph.join("__manifest").exists());
    assert!(temp.path().join("omnigraph.yaml").exists());
}

#[test]
fn repair_json_reports_noop_on_clean_graph() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    let output = output_success(cli().arg("repair").arg("--json").arg(&graph));
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["confirm"], false);
    assert_eq!(payload["force"], false);
    assert_eq!(payload["manifest_version"], Value::Null);
    let tables = payload["tables"].as_array().unwrap();
    assert_eq!(tables.len(), 4);
    assert!(tables.iter().all(|table| {
        table["classification"] == "no_drift" && table["action"] == "no_op"
    }));
}

#[test]
fn repair_confirm_json_refuses_suspicious_drift_with_nonzero_exit_then_force_succeeds() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);
    let graph_manifest_before = manifest_dataset_version(&graph);
    let (table_manifest_before, table_head_before) = forge_person_delete_drift(&graph);

    let refused = output_failure(
        cli()
            .arg("repair")
            .arg("--confirm")
            .arg("--json")
            .arg(&graph),
    );
    let refused_payload: Value = serde_json::from_slice(&refused.stdout).unwrap();
    assert_eq!(refused_payload["manifest_version"], Value::Null);
    let person = refused_payload["tables"]
        .as_array()
        .unwrap()
        .iter()
        .find(|table| table["table_key"] == "node:Person")
        .unwrap();
    assert_eq!(person["classification"], "suspicious");
    assert_eq!(person["action"], "refused");
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("repair refused"),
        "stderr should explain the non-zero exit; got: {}",
        String::from_utf8_lossy(&refused.stderr)
    );
    assert_eq!(manifest_dataset_version(&graph), graph_manifest_before);

    let forced = output_success(
        cli()
            .arg("repair")
            .arg("--force")
            .arg("--confirm")
            .arg("--json")
            .arg(&graph),
    );
    let forced_payload: Value = serde_json::from_slice(&forced.stdout).unwrap();
    let forced_manifest = forced_payload["manifest_version"].as_u64().unwrap();
    assert!(forced_manifest > graph_manifest_before);
    let person = forced_payload["tables"]
        .as_array()
        .unwrap()
        .iter()
        .find(|table| table["table_key"] == "node:Person")
        .unwrap();
    assert_eq!(person["classification"], "suspicious");
    assert_eq!(person["action"], "forced");
    assert_eq!(person["manifest_version"], table_manifest_before);
    assert_eq!(person["lance_head_version"], table_head_before);
    assert_eq!(manifest_dataset_version(&graph), forced_manifest);
}

#[test]
fn schema_plan_json_reports_supported_additive_change() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    let schema_path = temp.path().join("next.pg");
    init_graph(&graph);

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
            .arg(&graph),
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
    let graph = graph_path(temp.path());
    let schema_path = temp.path().join("breaking.pg");
    init_graph(&graph);

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
            .arg(&graph),
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
    let graph = graph_path(temp.path());
    let schema_path = temp.path().join("next.pg");
    init_graph(&graph);

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
            .arg(&graph),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["supported"], true);
    assert_eq!(payload["applied"], true);
    assert_eq!(payload["step_count"], 1);

    let db = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(Omnigraph::open(graph.to_string_lossy().as_ref()))
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
    let graph = graph_path(temp.path());
    let schema_path = fixture("test.pg");
    init_graph(&graph);

    let output = output_success(
        cli()
            .arg("schema")
            .arg("apply")
            .arg("--schema")
            .arg(&schema_path)
            .arg(&graph),
    );
    let stdout = stdout_string(&output);

    assert!(stdout.contains("applied: no"));
    assert!(stdout.contains("no schema changes"));
}

#[test]
fn schema_apply_json_renames_type_and_updates_snapshot() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    let schema_path = temp.path().join("rename.pg");
    init_graph(&graph);

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
            .arg(&graph),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["applied"], true);

    let db = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(Omnigraph::open(graph.to_string_lossy().as_ref()))
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
    let graph = graph_path(temp.path());
    let schema_path = temp.path().join("rename-property.pg");
    init_graph(&graph);

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
            .arg(&graph),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["applied"], true);

    let db = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(Omnigraph::open(graph.to_string_lossy().as_ref()))
        .unwrap();
    let person = &db.catalog().node_types["Person"];
    assert!(person.properties.contains_key("years"));
    assert!(!person.properties.contains_key("age"));
}

#[test]
fn schema_apply_json_adds_index_for_existing_property() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    let schema_path = temp.path().join("index.pg");
    init_graph(&graph);

    let before_index_count = tokio::runtime::Runtime::new().unwrap().block_on(async {
        let db = Omnigraph::open(graph.to_string_lossy().as_ref())
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
            .arg(&graph),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["applied"], true);

    let after_index_count = tokio::runtime::Runtime::new().unwrap().block_on(async {
        let db = Omnigraph::open(graph.to_string_lossy().as_ref())
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
    let graph = graph_path(temp.path());
    let schema_path = temp.path().join("breaking.pg");
    init_graph(&graph);

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
            .arg(&graph),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("changing property type"));
}

#[test]
fn schema_apply_rejects_when_non_main_branch_exists() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    let schema_path = temp.path().join("next.pg");
    init_graph(&graph);
    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--from")
            .arg("main")
            .arg("--uri")
            .arg(&graph)
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
            .arg(&graph),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("schema apply requires a graph with only main"));
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

/// `omnigraph lint` is the canonical top-level lint command after the
/// query/mutate rename. `omnigraph query lint` and `omnigraph query check`
/// are kept as deprecated argv shims (warning + rewrite). All three must
/// produce identical stdout output.
#[test]
fn lint_top_level_matches_deprecated_query_lint_output() {
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

    let canonical = output_success(
        cli()
            .arg("lint")
            .arg("--query")
            .arg(&query_path)
            .arg("--schema")
            .arg(&schema_path)
            .arg("--json"),
    );
    let deprecated_lint = output_success(
        cli()
            .arg("query")
            .arg("lint")
            .arg("--query")
            .arg(&query_path)
            .arg("--schema")
            .arg(&schema_path)
            .arg("--json"),
    );
    let deprecated_check = output_success(
        cli()
            .arg("query")
            .arg("check")
            .arg("--query")
            .arg(&query_path)
            .arg("--schema")
            .arg(&schema_path)
            .arg("--json"),
    );

    assert_eq!(stdout_string(&canonical), stdout_string(&deprecated_lint));
    assert_eq!(stdout_string(&canonical), stdout_string(&deprecated_check));

    // Canonical form must NOT emit the deprecation warning.
    let canonical_stderr = String::from_utf8(canonical.stderr).unwrap();
    assert!(
        !canonical_stderr.contains("deprecated"),
        "`omnigraph lint` is canonical and must not warn; got stderr: {canonical_stderr}"
    );

    // Deprecated forms MUST emit the one-line warning, pointing at the
    // new top-level `omnigraph lint`.
    let lint_stderr = String::from_utf8(deprecated_lint.stderr).unwrap();
    assert!(
        lint_stderr.contains("`omnigraph query lint` is deprecated")
            && lint_stderr.contains("`omnigraph lint`"),
        "expected deprecation warning pointing at `omnigraph lint`; got: {lint_stderr}"
    );
    let check_stderr = String::from_utf8(deprecated_check.stderr).unwrap();
    assert!(
        check_stderr.contains("`omnigraph query check` is deprecated")
            && check_stderr.contains("`omnigraph lint`"),
        "expected deprecation warning pointing at `omnigraph lint`; got: {check_stderr}"
    );
}

/// Bare `omnigraph check` is NOT a clap `visible_alias` on `lint` (MR-981 §6:
/// visible aliases give agents two canonical names to emit interchangeably).
/// It's an argv-level shim: rewrites to `omnigraph lint`, prints a one-line
/// stderr deprecation warning, and produces identical stdout to the canonical
/// invocation. Cargo/Go users typing `check` keep working; help text shows
/// only `lint`.
#[test]
fn deprecated_check_top_level_rewrites_to_lint() {
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

    let canonical = output_success(
        cli()
            .arg("lint")
            .arg("--query")
            .arg(&query_path)
            .arg("--schema")
            .arg(&schema_path)
            .arg("--json"),
    );
    let deprecated_check = output_success(
        cli()
            .arg("check")
            .arg("--query")
            .arg(&query_path)
            .arg("--schema")
            .arg(&schema_path)
            .arg("--json"),
    );

    assert_eq!(stdout_string(&canonical), stdout_string(&deprecated_check));

    let check_stderr = String::from_utf8(deprecated_check.stderr).unwrap();
    assert!(
        check_stderr.contains("`omnigraph check` is deprecated")
            && check_stderr.contains("`omnigraph lint`"),
        "expected `omnigraph check` deprecation warning pointing at `omnigraph lint`; got: {check_stderr}"
    );

    // `check` must NOT appear in the canonical `omnigraph --help` output —
    // agents copy the surface from help text and would otherwise emit both
    // names interchangeably.
    let help = cli().arg("--help").output().unwrap();
    let stdout = String::from_utf8(help.stdout).unwrap();
    let check_aliased = stdout
        .lines()
        .any(|line| line.trim_start().starts_with("lint") && line.contains("check"));
    assert!(
        !check_aliased,
        "`check` must not be advertised as a visible alias of `lint`; help output: {stdout}"
    );
}

/// `omnigraph read` and `omnigraph change` are kept as visible clap
/// aliases for the new canonical `query` / `mutate` subcommands, plus an
/// argv-level deprecation warning. The warning is emitted to stderr; the
/// command otherwise behaves identically to the canonical form.
#[test]
fn deprecated_read_and_change_subcommands_emit_warnings() {
    // Both subcommands require `--query`/`--query-string`/`--alias`, so
    // invoking them with no args will exit non-zero. That's fine --
    // we only care that the deprecation warning is printed before the
    // argument-required error.
    let output = cli().arg("read").output().unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("`omnigraph read` is deprecated") && stderr.contains("`omnigraph query`"),
        "expected `omnigraph read` deprecation warning; got: {stderr}"
    );

    let output = cli().arg("change").output().unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("`omnigraph change` is deprecated")
            && stderr.contains("`omnigraph mutate`"),
        "expected `omnigraph change` deprecation warning; got: {stderr}"
    );

    // Sanity check the inverse: the canonical names must NOT print the
    // deprecation banner.
    let output = cli().arg("query").arg("--help").output().unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        !stderr.contains("deprecated"),
        "`omnigraph query` is canonical and must not warn; got: {stderr}"
    );
    let output = cli().arg("mutate").arg("--help").output().unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        !stderr.contains("deprecated"),
        "`omnigraph mutate` is canonical and must not warn; got: {stderr}"
    );
}

#[test]
fn query_lint_can_use_local_graph_via_positional_uri() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    let query_path = temp.path().join("queries.gq");
    init_graph(&graph);
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
            .arg(&graph),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["status"], "ok");
    assert_eq!(payload["schema_source"]["kind"], "graph");
    assert_eq!(
        payload["schema_source"]["uri"].as_str(),
        Some(graph.to_string_lossy().as_ref())
    );
}

#[test]
fn query_lint_can_resolve_graph_and_query_from_config() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    let config_path = temp.path().join("omnigraph.yaml");
    init_graph(&graph);
    write_query_file(
        &temp.path().join("queries.gq"),
        r#"
query list_people() {
    match { $p: Person }
    return { $p.name }
}
"#,
    );
    write_config(&config_path, &local_yaml_config(&graph));

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
    assert_eq!(payload["schema_source"]["kind"], "graph");
    assert_eq!(
        payload["schema_source"]["uri"].as_str(),
        Some(graph.to_string_lossy().as_ref())
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
        stderr.contains("query lint is only supported against local graph URIs in this milestone")
    );
}

#[test]
fn query_lint_requires_schema_or_resolvable_graph_target() {
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
        stderr.contains("query lint requires --schema <schema.pg> or a resolvable graph target")
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
    let graph = graph_path(temp.path());
    init_graph(&graph);
    let data = fixture("test.jsonl");

    let output = output_success(
        cli()
            .arg("load")
            .arg("--data")
            .arg(&data)
            .arg("--json")
            .arg(&graph),
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
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--uri")
            .arg(&graph)
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
            .arg(&graph),
    );
    let stdout = stdout_string(&output);

    assert!(stdout.contains("branch feature"));
    assert!(stdout.contains("with merge"));
    assert!(stdout.contains("1 nodes across 1 node types"));
}

#[test]
fn read_json_outputs_rows_for_named_query() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);
    let queries = fixture("test.gq");

    let output = output_success(
        cli()
            .arg("read")
            .arg(&graph)
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
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--uri")
            .arg(&graph)
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
            .arg(&graph),
    );

    let output = output_success(
        cli()
            .arg("export")
            .arg(&graph)
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
  name: policy-test-graph
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
    let graph = graph_path(temp.path());
    let config = temp.path().join("omnigraph.yaml");
    init_graph(&graph);
    load_fixture(&graph);
    write_config(&config, &local_yaml_config(&graph));

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
    let graph = graph_path(temp.path());
    let config = temp.path().join("omnigraph.yaml");
    let query = temp.path().join("aliases.gq");
    init_graph(&graph);
    load_fixture(&graph);
    write_query_file(
        &query,
        &std::fs::read_to_string(fixture("test.gq")).unwrap(),
    );
    write_config(
        &config,
        &format!(
            "{}aliases:\n  owner:\n    command: read\n    query: aliases.gq\n    name: get_person\n    args: [name]\n    format: kv\n",
            local_yaml_config(&graph)
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
    let graph = graph_path(temp.path());
    let config = temp.path().join("omnigraph.yaml");
    let query = temp.path().join("aliases.gq");
    let data = temp.path().join("url-like.jsonl");
    init_graph(&graph);
    write_jsonl(
        &data,
        r#"{"type":"Person","data":{"name":"https://example.com","age":30}}"#,
    );
    output_success(cli().arg("load").arg("--data").arg(&data).arg(&graph));
    write_query_file(
        &query,
        &std::fs::read_to_string(fixture("test.gq")).unwrap(),
    );
    write_config(
        &config,
        &format!(
            "graphs:\n  local:\n    uri: '{}'\nquery:\n  roots:\n    - .\npolicy: {{}}\naliases:\n  owner:\n    command: read\n    query: aliases.gq\n    name: get_person\n    args: [name]\n    graph: local\n    format: kv\n",
            graph.to_string_lossy()
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
    let graph = graph_path(temp.path());
    let config = temp.path().join("omnigraph.yaml");
    let query = temp.path().join("mutations.gq");
    init_graph(&graph);
    load_fixture(&graph);
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
            local_yaml_config(&graph)
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
            .arg(&graph)
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
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    let output = output_success(
        cli()
            .arg("read")
            .arg(&graph)
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
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    let output = output_success(
        cli()
            .arg("read")
            .arg(&graph)
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
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);
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
            .arg(&graph)
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
            .arg(&graph)
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
    let graph = graph_path(temp.path());
    let config = temp.path().join("omnigraph.yaml");
    init_graph(&graph);
    load_fixture(&graph);
    write_config(&config, &local_yaml_config(&graph));
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
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    let output = output_failure(
        cli()
            .arg("read")
            .arg(&graph)
            .arg("--query")
            .arg(fixture("test.gq")),
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("multiple queries"));
}

#[test]
fn read_supports_inline_query_string() {
    let temp = tempdir().unwrap();
    let repo = graph_path(temp.path());
    init_graph(&repo);
    load_fixture(&repo);

    let output = output_success(
        cli()
            .arg("read")
            .arg(&repo)
            .arg("-e")
            .arg("query find($name: String) { match { $p: Person { name: $name } } return { $p.name, $p.age } }")
            .arg("--params")
            .arg(r#"{"name":"Alice"}"#)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["query_name"], "find");
    assert_eq!(payload["row_count"], 1);
    assert_eq!(payload["rows"][0]["p.name"], "Alice");
}

#[test]
fn change_supports_inline_query_string() {
    let temp = tempdir().unwrap();
    let repo = graph_path(temp.path());
    init_graph(&repo);
    load_fixture(&repo);

    let output = output_success(
        cli()
            .arg("change")
            .arg(&repo)
            .arg("--query-string")
            .arg("query add($name: String, $age: I32) { insert Person { name: $name, age: $age } }")
            .arg("--params")
            .arg(r#"{"name":"Inline","age":42}"#)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["query_name"], "add");
    assert_eq!(payload["affected_nodes"], 1);

    let verify = output_success(
        cli()
            .arg("read")
            .arg(&repo)
            .arg("-e")
            .arg("query find($name: String) { match { $p: Person { name: $name } } return { $p.name } }")
            .arg("--params")
            .arg(r#"{"name":"Inline"}"#)
            .arg("--json"),
    );
    let verify_payload: Value = serde_json::from_slice(&verify.stdout).unwrap();
    assert_eq!(verify_payload["row_count"], 1);
}

#[test]
fn read_rejects_query_string_combined_with_query() {
    let temp = tempdir().unwrap();
    let repo = graph_path(temp.path());
    init_graph(&repo);
    load_fixture(&repo);

    let output = output_failure(
        cli()
            .arg("read")
            .arg(&repo)
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("-e")
            .arg("query whatever() { match { $p: Person } return { $p.name } }"),
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("cannot be used") || stderr.contains("conflict"),
        "expected clap conflict error, got: {stderr}"
    );
}

#[test]
fn read_rejects_empty_query_string() {
    let temp = tempdir().unwrap();
    let repo = graph_path(temp.path());
    init_graph(&repo);
    load_fixture(&repo);

    let output = output_failure(cli().arg("read").arg(&repo).arg("-e").arg(""));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("must not be empty"),
        "expected empty-string rejection, got: {stderr}"
    );
}

#[test]
fn branch_create_json_outputs_source_and_name() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);

    let output = output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--uri")
            .arg(&graph)
            .arg("--from")
            .arg("main")
            .arg("feature")
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["from"], "main");
    assert_eq!(payload["name"], "feature");
    assert_eq!(payload["uri"], graph.to_string_lossy().as_ref());
}

#[test]
fn branch_list_outputs_sorted_branches() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);

    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--uri")
            .arg(&graph)
            .arg("--from")
            .arg("main")
            .arg("zeta"),
    );
    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--uri")
            .arg(&graph)
            .arg("--from")
            .arg("main")
            .arg("alpha"),
    );

    let output = output_success(cli().arg("branch").arg("list").arg("--uri").arg(&graph));
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
    let graph = graph_path(temp.path());
    init_graph(&graph);

    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--uri")
            .arg(&graph)
            .arg("--from")
            .arg("main")
            .arg("feature"),
    );

    let output = output_success(
        cli()
            .arg("branch")
            .arg("delete")
            .arg("--uri")
            .arg(&graph)
            .arg("feature")
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["name"], "feature");
    assert_eq!(payload["uri"], graph.to_string_lossy().as_ref());

    let listed = output_success(cli().arg("branch").arg("list").arg("--uri").arg(&graph));
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
    let graph = graph_path(temp.path());
    init_graph(&graph);

    let output = output_failure(
        cli()
            .arg("branch")
            .arg("delete")
            .arg("--uri")
            .arg(&graph)
            .arg("main"),
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("cannot delete branch 'main'"));
}

#[test]
fn branch_merge_defaults_target_to_main() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--uri")
            .arg(&graph)
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
            .arg(&graph),
    );

    let merge_output = output_success(
        cli()
            .arg("branch")
            .arg("merge")
            .arg("--uri")
            .arg(&graph)
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
            .arg(&graph)
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
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--uri")
            .arg(&graph)
            .arg("--from")
            .arg("main")
            .arg("feature"),
    );
    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--uri")
            .arg(&graph)
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
            .arg(&graph),
    );

    let merge_output = output_success(
        cli()
            .arg("branch")
            .arg("merge")
            .arg("--uri")
            .arg(&graph)
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
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    let output = output_success(cli().arg("snapshot").arg(&graph).arg("--json"));
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["branch"], "main");
    assert_eq!(
        payload["manifest_version"].as_u64().unwrap(),
        manifest_dataset_version(&graph)
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
    let graph = graph_path(temp.path());
    let config = temp.path().join("omnigraph.yaml");
    init_graph(&graph);
    load_fixture(&graph);
    write_config(&config, &local_yaml_config(&graph));

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
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    let output = output_success(cli().arg("snapshot").arg(&graph));
    let stdout = stdout_string(&output);

    assert!(stdout.contains("branch: main"));
    assert!(stdout.contains("manifest_version:"));
    assert!(stdout.contains("node:Person v"));
    assert!(stdout.contains("edge:Knows v"));
}

#[test]
fn commit_show_accepts_long_uri_flag() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    let list = output_success(cli().arg("commit").arg("list").arg(&graph).arg("--json"));
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
            .arg(&graph)
            .arg(&commit_id)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["graph_commit_id"], commit_id);
    assert!(payload["manifest_version"].as_u64().unwrap() >= 1);
}

#[test]
fn cli_fails_for_missing_graph() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());

    let output = output_failure(cli().arg("snapshot").arg(&graph));
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
    let graph = graph_path(temp.path());
    let missing_schema = temp.path().join("missing.pg");
    let missing_data = temp.path().join("missing.jsonl");

    let init_output = output_failure(
        cli()
            .arg("init")
            .arg("--schema")
            .arg(&missing_schema)
            .arg(&graph),
    );
    assert!(
        String::from_utf8(init_output.stderr)
            .unwrap()
            .contains("No such file")
    );

    init_graph(&graph);
    let load_output = output_failure(
        cli()
            .arg("load")
            .arg("--data")
            .arg(&missing_data)
            .arg(&graph),
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
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    let missing_branch = output_failure(
        cli()
            .arg("branch")
            .arg("merge")
            .arg("--uri")
            .arg(&graph)
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
            .arg(&graph)
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

// `omnigraph run list/show/publish/abort` subcommands removed
// alongside the run state machine. Direct-to-target writes leave nothing
// for these CLIs to manage. Audit history is now visible via
// `omnigraph commit list` reading the commit graph.

// ─── MR-694 PR B: --allow-data-loss flag end-to-end ──────────────────────
//
// The schema-lint chassis v1.2 (PR #100) shipped the `--allow-data-loss`
// flag at the CLI layer; the SDK suite verifies promotion to Hard mode
// via `apply_schema_with_options(.., SchemaApplyOptions { allow_data_loss })`.
// These CLI tests close the integration gap so a future change that
// drops the flag wiring in `main.rs` turns red.

#[test]
fn schema_apply_allow_data_loss_flag_promotes_drops_to_hard() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    let schema_path = temp.path().join("drop-age.pg");
    init_graph(&graph);

    // Drop the nullable `age` column.
    let next_schema = fs::read_to_string(fixture("test.pg"))
        .unwrap()
        .replace("    age: I32?\n", "");
    fs::write(&schema_path, next_schema).unwrap();

    let output = output_success(
        cli()
            .arg("schema")
            .arg("apply")
            .arg("--schema")
            .arg(&schema_path)
            .arg("--allow-data-loss")
            .arg("--json")
            .arg(&graph),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["applied"], true);

    let drop_step = payload["steps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["kind"] == "drop_property")
        .expect("plan should include a drop_property step");
    assert_eq!(
        drop_step["mode"], "hard",
        "--allow-data-loss should promote Soft → Hard; full step: {drop_step}",
    );
}

#[test]
fn schema_apply_without_allow_data_loss_keeps_soft_drops() {
    // Symmetric to the above: same schema change without the flag →
    // drops stay Soft. Pins default semantics against accidental Hard
    // promotion if a future refactor changes the option threading.
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    let schema_path = temp.path().join("drop-age-soft.pg");
    init_graph(&graph);

    let next_schema = fs::read_to_string(fixture("test.pg"))
        .unwrap()
        .replace("    age: I32?\n", "");
    fs::write(&schema_path, next_schema).unwrap();

    let output = output_success(
        cli()
            .arg("schema")
            .arg("apply")
            .arg("--schema")
            .arg(&schema_path)
            .arg("--json")
            .arg(&graph),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["applied"], true);

    let drop_step = payload["steps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["kind"] == "drop_property")
        .expect("plan should include a drop_property step");
    assert_eq!(
        drop_step["mode"], "soft",
        "no flag should leave drops Soft; full step: {drop_step}",
    );
}

#[test]
fn schema_plan_parity_cli_and_sdk() {
    // Same .pg through `Omnigraph::plan_schema_with_options` (SDK) and
    // `omnigraph schema plan --json` (CLI). Asserts the steps array is
    // byte-identical after JSON round-trip. HTTP doesn't expose a
    // separate /schema/plan route — that side of parity is covered by
    // the HTTP soft/hard drop tests, which exercise apply with
    // identical fixtures.
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    let schema_path = temp.path().join("plan-parity.pg");
    let next_schema = fs::read_to_string(fixture("test.pg")).unwrap().replace(
        "    age: I32?\n}",
        "    age: I32?\n    nickname: String?\n}",
    );
    fs::write(&schema_path, &next_schema).unwrap();

    // CLI side.
    let cli_output = output_success(
        cli()
            .arg("schema")
            .arg("plan")
            .arg("--schema")
            .arg(&schema_path)
            .arg("--json")
            .arg(&graph),
    );
    let cli_payload: Value = serde_json::from_slice(&cli_output.stdout).unwrap();

    // SDK side: open graph, call plan_schema.
    let plan = tokio::runtime::Runtime::new().unwrap().block_on(async {
        let db = Omnigraph::open(graph.to_string_lossy().as_ref())
            .await
            .unwrap();
        db.plan_schema(&next_schema).await.unwrap()
    });
    let sdk_steps = serde_json::to_value(&plan.steps).unwrap();

    assert_eq!(
        cli_payload["steps"], sdk_steps,
        "CLI plan steps must match SDK plan steps for identical input",
    );
    assert_eq!(cli_payload["supported"], plan.supported);
}

// ─── MR-668 PR 8 — omnigraph graphs subcommand ─────────────────────────────

/// `omnigraph graphs --help` lists only the read-only `list`
/// subcommand. Runtime add (`create`) and remove (`delete`) are
/// deferred — operators add/remove graphs by editing `omnigraph.yaml`
/// and restarting. This test pins the deferral against accidental
/// re-introduction.
#[test]
fn graphs_subcommand_help_lists_list_only() {
    let output = output_success(cli().arg("graphs").arg("--help"));
    let stdout = stdout_string(&output);
    assert!(
        stdout.contains("list"),
        "expected `list` subcommand in help output:\n{stdout}"
    );
    let lowered = stdout.to_lowercase();
    assert!(
        !lowered.contains("create a new graph"),
        "graph create should not be in v0.6.0 help; got:\n{stdout}"
    );
    assert!(
        !lowered.contains("delete a graph"),
        "graph delete should not be in v0.6.0 help; got:\n{stdout}"
    );
}

/// `omnigraph graphs list` against a local URI errors with a clear
/// message — the CLI only operates against remote multi-graph servers.
#[test]
fn graphs_list_against_local_uri_errors_with_remote_only_message() {
    let output = output_failure(
        cli()
            .arg("graphs")
            .arg("list")
            .arg("--uri")
            .arg("/tmp/local"),
    );
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        stderr.contains("remote multi-graph server URL"),
        "expected 'remote multi-graph server URL' rejection in stderr; got:\n{stderr}"
    );
}

fn queries_test_config(graph_uri: &str, entry: &str, gq_file: &str) -> String {
    format!(
        "graphs:\n  local:\n    uri: '{}'\n    queries:\n      {entry}:\n        file: ./{gq_file}\n\
         cli:\n  graph: local\npolicy: {{}}\n",
        graph_uri.replace('\'', "''")
    )
}

#[test]
fn queries_validate_exits_zero_on_clean_registry() {
    let graph = SystemGraph::loaded();
    graph.write_query(
        "find_person.gq",
        "query find_person($name: String) { match { $p: Person { name: $name } } return { $p.age } }",
    );
    let config = graph.write_config(
        "omnigraph.yaml",
        &queries_test_config(
            &graph.path().to_string_lossy(),
            "find_person",
            "find_person.gq",
        ),
    );
    let output = output_success(
        cli()
            .arg("queries")
            .arg("validate")
            .arg("--config")
            .arg(&config),
    );
    let stdout = stdout_string(&output);
    assert!(stdout.contains("OK"), "stdout:\n{stdout}");
}

#[test]
fn queries_validate_exits_nonzero_on_type_broken_query() {
    let graph = SystemGraph::loaded();
    // `Widget` is not in the fixture schema.
    graph.write_query(
        "ghost.gq",
        "query ghost() { match { $w: Widget } return { $w.name } }",
    );
    let config = graph.write_config(
        "omnigraph.yaml",
        &queries_test_config(&graph.path().to_string_lossy(), "ghost", "ghost.gq"),
    );
    let output = output_failure(
        cli()
            .arg("queries")
            .arg("validate")
            .arg("--config")
            .arg(&config),
    );
    let stdout = stdout_string(&output);
    assert!(
        stdout.contains("ghost"),
        "validation should name the broken query; stdout:\n{stdout}"
    );
}

#[test]
fn queries_list_prints_registered_query() {
    let graph = SystemGraph::loaded();
    graph.write_query(
        "find_person.gq",
        "query find_person($name: String) { match { $p: Person { name: $name } } return { $p.age } }",
    );
    // Exposed with an explicit tool name so the list shows the MCP suffix.
    let config = graph.write_config(
        "omnigraph.yaml",
        &format!(
            concat!(
                "graphs:\n",
                "  local:\n",
                "    uri: '{}'\n",
                "    queries:\n",
                "      find_person:\n",
                "        file: ./find_person.gq\n",
                "        mcp: {{ expose: true, tool_name: lookup_person }}\n",
                "cli:\n",
                "  graph: local\n",
                "policy: {{}}\n",
            ),
            graph.path().to_string_lossy().replace('\'', "''")
        ),
    );
    let output = output_success(
        cli()
            .arg("queries")
            .arg("list")
            .arg("--config")
            .arg(&config),
    );
    let stdout = stdout_string(&output);
    assert!(stdout.contains("find_person"), "stdout:\n{stdout}");
    assert!(
        stdout.contains("$name: String"),
        "list should show typed params; stdout:\n{stdout}"
    );
    assert!(
        stdout.contains("[mcp: lookup_person]"),
        "list should show the MCP tool name for exposed queries; stdout:\n{stdout}"
    );
}

#[test]
fn queries_list_requires_graph_selection_for_per_graph_only_registries() {
    let graph = SystemGraph::loaded();
    graph.write_query(
        "find_person.gq",
        "query find_person($name: String) { match { $p: Person { name: $name } } return { $p.age } }",
    );
    let config = graph.write_config(
        "omnigraph.yaml",
        &format!(
            concat!(
                "graphs:\n",
                "  local:\n",
                "    uri: '{}'\n",
                "    queries:\n",
                "      find_person:\n",
                "        file: ./find_person.gq\n",
                "policy: {{}}\n",
            ),
            graph.path().to_string_lossy().replace('\'', "''")
        ),
    );

    let output = output_failure(
        cli()
            .arg("queries")
            .arg("list")
            .arg("--config")
            .arg(&config),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("local") && stderr.contains("--target local"),
        "error must name the graph and give a concrete selection hint; stderr:\n{stderr}"
    );
}

#[test]
fn queries_list_without_graph_selection_lists_top_level_registry() {
    let graph = SystemGraph::loaded();
    graph.write_query(
        "top_find.gq",
        "query top_find($name: String) { match { $p: Person { name: $name } } return { $p.age } }",
    );
    let config = graph.write_config(
        "omnigraph.yaml",
        concat!(
            "queries:\n",
            "  top_find:\n",
            "    file: ./top_find.gq\n",
            "policy: {}\n",
        ),
    );

    let output = output_success(
        cli()
            .arg("queries")
            .arg("list")
            .arg("--config")
            .arg(&config),
    );
    let stdout = stdout_string(&output);
    assert!(stdout.contains("top_find"), "stdout:\n{stdout}");
}

#[test]
fn queries_list_unknown_target_errors() {
    // `queries list` opens no graph URI, so unknown-graph validation can't ride
    // along on URI resolution the way it does for every other command. An
    // unknown `--target` must still error (naming the graph) instead of
    // silently falling back to the top-level registry and showing the wrong
    // (or empty) catalog.
    let graph = SystemGraph::loaded();
    graph.write_query(
        "find_person.gq",
        "query find_person($name: String) { match { $p: Person { name: $name } } return { $p.age } }",
    );
    let config = graph.write_config(
        "omnigraph.yaml",
        &queries_test_config(
            &graph.path().to_string_lossy(),
            "find_person",
            "find_person.gq",
        ),
    );
    let output = output_failure(
        cli()
            .arg("queries")
            .arg("list")
            .arg("--target")
            .arg("nonexistent")
            .arg("--config")
            .arg(&config),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("nonexistent"),
        "error must name the unknown graph; stderr:\n{stderr}"
    );
}

#[test]
fn queries_commands_reject_named_graph_with_populated_top_level_block() {
    // A named graph (here via `cli.graph`) uses its own `graphs.<name>` block,
    // so a populated top-level `queries:` block would be silently ignored — a
    // config the server REFUSES to boot. `queries validate`/`list` must reject
    // it too (matching boot) instead of validating/listing the per-graph block
    // and giving a false green.
    let graph = SystemGraph::loaded();
    graph.write_query(
        "find_person.gq",
        "query find_person($name: String) { match { $p: Person { name: $name } } return { $p.age } }",
    );
    let config = graph.write_config(
        "omnigraph.yaml",
        &format!(
            concat!(
                "graphs:\n",
                "  local:\n",
                "    uri: '{}'\n",
                "    queries:\n",
                "      find_person:\n",
                "        file: ./find_person.gq\n",
                "cli:\n",
                "  graph: local\n",
                "queries:\n", // populated top-level block: the coherence violation
                "  legacy:\n",
                "    file: ./legacy.gq\n",
                "policy: {{}}\n",
            ),
            graph.path().to_string_lossy().replace('\'', "''")
        ),
    );
    // Both resolve `local` from cli.graph (no positional URI), so both must
    // error and name the graph + the ignored block — like server boot does.
    for sub in ["validate", "list"] {
        let output = output_failure(cli().arg("queries").arg(sub).arg("--config").arg(&config));
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("local") && stderr.contains("queries"),
            "`queries {sub}` must reject a named graph with a populated top-level block; stderr:\n{stderr}"
        );
    }
}

#[test]
fn queries_validate_exits_nonzero_on_duplicate_tool_name() {
    // Two exposed queries claiming one MCP tool name is a load-time
    // collision — `queries validate` must fail (offline, before the engine
    // opens) and name both queries plus the contested tool.
    let graph = SystemGraph::loaded();
    graph.write_query(
        "a.gq",
        "query a() { match { $p: Person } return { $p.name } }",
    );
    graph.write_query(
        "b.gq",
        "query b() { match { $p: Person } return { $p.name } }",
    );
    let config = graph.write_config(
        "omnigraph.yaml",
        &format!(
            concat!(
                "graphs:\n",
                "  local:\n",
                "    uri: '{}'\n",
                "    queries:\n",
                "      a:\n",
                "        file: ./a.gq\n",
                "        mcp: {{ expose: true, tool_name: dup }}\n",
                "      b:\n",
                "        file: ./b.gq\n",
                "        mcp: {{ expose: true, tool_name: dup }}\n",
                "cli:\n",
                "  graph: local\n",
                "policy: {{}}\n",
            ),
            graph.path().to_string_lossy().replace('\'', "''")
        ),
    );
    let output = output_failure(
        cli()
            .arg("queries")
            .arg("validate")
            .arg("--config")
            .arg(&config),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("dup") && stderr.contains("'a'") && stderr.contains("'b'"),
        "duplicate tool name should be reported naming both queries; stderr:\n{stderr}"
    );
}

#[test]
fn queries_validate_positional_uri_ignores_default_graph() {
    // A positional URI is anonymous → the schema AND the registry both come
    // from top-level, even when `cli.graph` names a graph whose per-graph
    // queries would fail. Pins that the URI and registry can't diverge.
    let graph = SystemGraph::loaded();
    graph.write_query(
        "clean.gq",
        "query clean($name: String) { match { $p: Person { name: $name } } return { $p.age } }",
    );
    // `Widget` is not in the fixture schema — the default graph's per-graph
    // query would break validate if it were (wrongly) selected.
    graph.write_query(
        "broken.gq",
        "query broken() { match { $w: Widget } return { $w.name } }",
    );
    let config = graph.write_config(
        "omnigraph.yaml",
        concat!(
            "cli:\n  graph: prod\n",
            "graphs:\n",
            "  prod:\n",
            "    uri: /nonexistent-prod.omni\n",
            "    queries:\n",
            "      broken:\n",
            "        file: ./broken.gq\n",
            "queries:\n",
            "  clean:\n",
            "    file: ./clean.gq\n",
            "policy: {}\n",
        ),
    );
    // Positional URI = the real loaded graph; selection is anonymous, so the
    // CLEAN top-level registry validates (not prod's broken one).
    let output = output_success(
        cli()
            .arg("queries")
            .arg("validate")
            .arg(graph.path())
            .arg("--config")
            .arg(&config),
    );
    let stdout = stdout_string(&output);
    assert!(
        stdout.contains("OK"),
        "positional URI must validate the top-level registry, not the cli.graph default; stdout:\n{stdout}"
    );
}
