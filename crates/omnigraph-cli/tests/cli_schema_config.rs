//! init/config scaffolding, schema plan/apply, graphs listing, version.
//! Moved verbatim from tests/cli.rs in the modularization.

use std::fs;

use lance::index::DatasetIndexExt;
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
