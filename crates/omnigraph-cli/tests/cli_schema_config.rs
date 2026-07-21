//! init/config scaffolding, schema plan/apply, graphs listing, version.
//! Moved verbatim from tests/cli.rs in the modularization.

use std::fs;

use omnigraph::db::{Omnigraph, ReadTarget};
use serde_json::Value;
use tempfile::tempdir;

mod support;

use support::*;


#[test]
fn version_command_prints_current_cli_version() {
    let output = output_success(cli().arg("version"));
    let stdout = stdout_string(&output);

    assert!(
        stdout.contains(&format!("omnigraph {}", env!("CARGO_PKG_VERSION"))),
        "version output must include the CLI version line, got: {stdout}"
    );
    assert!(
        stdout.contains(&format!(
            "internal-schema {}",
            omnigraph::db::manifest::INTERNAL_MANIFEST_SCHEMA_VERSION
        )),
        "version output must include the internal-schema line, got: {stdout}"
    );
}

#[test]
fn help_groups_commands_by_capability() {
    // RFC-010 Slice 2 / RFC-011 Slice B: `--help` clusters commands (declaration
    // order in the Command enum) and explains the capability each needs in an
    // after_help legend. Pinned lightly — the legend phrase + the cluster
    // ordering — to avoid brittle full-text assertions on clap's help body.
    let output = output_success(cli().arg("--help"));
    let stdout = stdout_string(&output);

    assert!(
        stdout.contains("COMMANDS BY CAPABILITY"),
        "capability legend (after_help) missing from --help:\n{stdout}"
    );

    // The Commands list precedes the legend, so first occurrences sit in the
    // list and must appear in order: an `any` data verb, then a `direct` verb,
    // then the `control` verb.
    let pos = |needle: &str| {
        stdout
            .find(needle)
            .unwrap_or_else(|| panic!("'{needle}' not found in --help:\n{stdout}"))
    };
    assert!(
        pos("query") < pos("optimize"),
        "data (any) commands should be listed before direct commands"
    );
    assert!(
        pos("optimize") < pos("cluster"),
        "direct commands should be listed before the control command"
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
    // RFC-008 stage 3: init no longer scaffolds the legacy config file.
    assert!(!temp.path().join("omnigraph.yaml").exists());
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
fn schema_plan_with_server_flag_errors_wrong_plane() {
    // RFC-010 Slice 1: `schema plan` is storage-plane while `schema show/apply`
    // are data-plane — the guard rejects --server on plan with the per-subcommand
    // label (proving command_plane/command_label descend into the nested enum).
    let output = output_failure(
        cli()
            .arg("schema")
            .arg("plan")
            .arg("--schema")
            .arg(fixture("test.pg"))
            .arg("--server")
            .arg("prod"),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("`schema plan` is a direct (storage-native) command")
            && stderr.contains("Pass a storage URI."),
        "schema plan wrong-capability message not found; got: {stderr}"
    );
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
    // iss-848: `schema apply` records the `@index` intent but defers the physical
    // index build (materialized later by ensure_indices/optimize; on this empty
    // table nothing builds anyway). So the physical index count is unchanged.
    assert_eq!(
        after_index_count, before_index_count,
        "schema apply records @index intent but defers the physical build (iss-848)"
    );
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
fn init_with_store_flag_errors_instead_of_ignoring_it() {
    // `init` takes its target as a required positional URI and never reads
    // `--store`; passing both must be a loud guard error, not a silently
    // ignored second address (PR #377 review follow-up).
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    let schema = fixture("test.pg");
    let output = output_failure(
        cli()
            .arg("init")
            .arg("--schema")
            .arg(&schema)
            .arg("--store")
            .arg("file:///elsewhere/graph.omni")
            .arg(&graph),
    );
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        stderr.contains("`init` is a direct (storage-native) command")
            && stderr.contains("--store")
            && stderr.contains("does not apply"),
        "expected the addressing-guard store rejection on init; got:\n{stderr}"
    );
    assert!(
        !graph.exists(),
        "init must not run when the addressing is rejected"
    );
}

#[test]
fn init_with_profile_flag_errors_instead_of_ignoring_it() {
    // `init` never resolves a scope, so an explicit `--profile` (which may
    // carry a store binding) would be silently discarded — the same
    // two-address ambiguity as `init --store` (PR #377 review follow-up).
    // The ambient $OMNIGRAPH_PROFILE default remains ignored, matching the
    // explicit-flag-vs-config-default rule on the registry path.
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    let schema = fixture("test.pg");
    let output = output_failure(
        cli()
            .arg("init")
            .arg("--schema")
            .arg(&schema)
            .arg("--profile")
            .arg("localdev")
            .arg(&graph),
    );
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        stderr.contains("`init` is a direct (storage-native) command")
            && stderr.contains("--profile")
            && stderr.contains("does not apply"),
        "expected the addressing-guard profile rejection on init; got:\n{stderr}"
    );
    assert!(
        !graph.exists(),
        "init must not run when the addressing is rejected"
    );
}

#[test]
fn graphs_list_rejects_store_scope() {
    // `graphs list` is a served-registry command: it enumerates a server's
    // graphs, so a `--store` (local) address can never apply. The addressing
    // guard rejects it up front instead of failing late in the client with an
    // engine-limitation message.
    let output = output_failure(
        cli()
            .arg("graphs")
            .arg("list")
            .arg("--store")
            .arg("/tmp/local"),
    );
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        stderr.contains("`graphs list` is a served command")
            && stderr.contains(
                "--store addresses a single graph's storage directly and does not apply"
            )
            && stderr.contains("Address the server with --server <name|url> or --profile <name>."),
        "expected the addressing-guard store rejection in stderr; got:\n{stderr}"
    );
}

#[test]
fn graphs_list_rejects_graph_selector() {
    // `graphs list` IS the enumeration — selecting a graph is meaningless, and
    // historically `--graph` corrupted the URL to `/graphs/<id>/graphs` (404).
    // The guard rejects it before any network I/O.
    let output = output_failure(
        cli()
            .arg("graphs")
            .arg("list")
            .arg("--server")
            .arg("http://127.0.0.1:9")
            .arg("--graph")
            .arg("atlas"),
    );
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        stderr.contains("`graphs list` is a served command")
            && stderr.contains(
                "--graph selects a graph within a server or cluster scope and does not apply"
            )
            && stderr.contains("Address the server with --server <name|url> or --profile <name>."),
        "expected the addressing-guard graph rejection in stderr; got:\n{stderr}"
    );
}

/// Operator home for the graphs-list registry-resolution tests: a store-bound
/// and a cluster-bound profile, neither of which can carry a served-registry
/// command.
fn registry_profile_home() -> tempfile::TempDir {
    let home = tempfile::tempdir().unwrap();
    std::fs::write(
        home.path().join("config.yaml"),
        "servers:\n  prod:\n    url: https://graph.example.com\n\
         clusters:\n  brain:\n    root: s3://acme/clusters/brain\n\
         profiles:\n\
         \x20 localdev:\n    store: file:///data/dev.omni\n\
         \x20 brain-admin:\n    cluster: brain\n",
    )
    .unwrap();
    home
}

#[test]
fn graphs_list_without_scope_needs_server() {
    // No addressing and no operator defaults: the served-registry resolver
    // asks for a server scope — not the graph-shaped "no graph addressed"
    // advice, which points at flags graphs list cannot consume.
    let output = output_failure(cli().arg("graphs").arg("list"));
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        stderr.contains("`graphs list` needs a server scope")
            && stderr.contains("--server <name|url> or --profile <name>"),
        "expected the needs-a-server-scope error in stderr; got:\n{stderr}"
    );
}

#[test]
fn graphs_list_rejects_store_bound_profile() {
    // A store-bound profile resolves a single graph's storage; the registry
    // is server-scoped, so resolution fails with scope-shaped advice instead
    // of the late embedded-arm engine-limitation message.
    let home = registry_profile_home();
    let output = output_failure(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .arg("graphs")
            .arg("list")
            .arg("--profile")
            .arg("localdev"),
    );
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        stderr.contains("requires a server")
            && stderr.contains("profile 'localdev' resolves a store scope"),
        "expected the served store-scope rejection in stderr; got:\n{stderr}"
    );
}

#[test]
fn graphs_list_rejects_cluster_bound_profile() {
    // A cluster-bound profile is control-plane addressing; the rejection
    // points at `cluster status` for cluster-graph enumeration instead of the
    // misleading graph-data-command error.
    let home = registry_profile_home();
    let output = output_failure(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .arg("graphs")
            .arg("list")
            .arg("--profile")
            .arg("brain-admin"),
    );
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        stderr.contains("requires a server")
            && stderr.contains("profile 'brain-admin' resolves a cluster scope")
            && stderr.contains("cluster status"),
        "expected the served cluster-scope rejection in stderr; got:\n{stderr}"
    );
}

#[test]
fn graphs_list_rejects_as_actor() {
    // The registry read carries no actor; `--as` is for direct-engine and
    // cluster writes. Rejected loudly instead of silently ignored.
    let output = output_failure(cli().arg("graphs").arg("list").arg("--as").arg("act-op"));
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        stderr.contains("`graphs list` is a served command")
            && stderr.contains(
                "--as sets the actor for a direct-engine or cluster write and does not apply"
            ),
        "expected the addressing-guard --as rejection in stderr; got:\n{stderr}"
    );
}
