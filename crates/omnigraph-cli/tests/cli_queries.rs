//! Stored-query commands and alias resolution.
//! Moved verbatim from tests/cli.rs in the modularization.

use std::path::{Path, PathBuf};

use serde_json::Value;
use tempfile::{TempDir, tempdir};

mod support;

use support::*;

const STATEMENT_GRAPH_ID: &str = "knowledge";

/// A loaded graph for the embedded (`--store`) arm.
fn loaded_graph() -> (TempDir, PathBuf) {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);
    (temp, graph)
}

/// A loaded graph served unauthenticated for the remote (`--server`) arm.
fn served_graph() -> (ClusterFixture, TestServer) {
    let cluster = converged_loaded_cluster(STATEMENT_GRAPH_ID, None);
    let server = spawn_server_with_cluster(cluster.path());
    (cluster, server)
}

fn embedded(verb: &str, graph: &Path) -> assert_cmd::Command {
    let mut command = cli();
    command.arg(verb).arg("--store").arg(graph);
    command
}

fn served(verb: &str, server: &TestServer) -> assert_cmd::Command {
    let mut command = cli();
    command
        .arg(verb)
        .arg("--server")
        .arg(&server.base_url)
        .arg("--graph")
        .arg(STATEMENT_GRAPH_ID);
    command
}

fn stderr_string(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// `branch list` rows as names, from the statement's `--json` envelope.
fn listed_names(output: &std::process::Output) -> Vec<String> {
    parse_stdout_json(output)["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["name"].as_str().unwrap().to_string())
        .collect()
}

const SET_AGE: &str = "query set_age($name: String, $age: I32) { update Person set { age: $age } where name = $name }";

#[test]
fn branch_statements_embedded_print_the_verb_lines() {
    let (_temp, graph) = loaded_graph();

    let created = output_success(embedded("mutate", &graph).arg("-e").arg("branch create b0"));
    assert_eq!(stdout_string(&created), "created branch b0 from main\n");

    let listed = output_success(
        embedded("query", &graph)
            .arg("-e")
            .arg("branch list")
            .arg("--json"),
    );
    assert_eq!(listed_names(&listed), ["b0", "main"]);

    let up_to_date = output_success(
        embedded("mutate", &graph)
            .arg("-e")
            .arg("branch merge b0 into main"),
    );
    assert_eq!(
        stdout_string(&up_to_date),
        "merged b0 into main: already_up_to_date\n"
    );

    let defaulted = output_success(embedded("mutate", &graph).arg("-e").arg("branch merge b0"));
    assert_eq!(
        stdout_string(&defaulted),
        "merged b0 into main: already_up_to_date\n",
        "a merge with no `into` targets main, and the line names it"
    );

    output_success(
        embedded("mutate", &graph)
            .arg("--branch")
            .arg("b0")
            .arg("-e")
            .arg(SET_AGE)
            .arg("--params")
            .arg(r#"{"name":"Alice","age":41}"#),
    );
    let fast_forward = output_success(
        embedded("mutate", &graph)
            .arg("-e")
            .arg("branch merge b0 into main")
            .arg("--json"),
    );
    let payload = parse_stdout_json(&fast_forward);
    assert_eq!(payload["outcome"]["kind"], "merged");
    assert_eq!(payload["outcome"]["source"], "b0");
    assert_eq!(payload["outcome"]["target"], "main");
    assert_eq!(payload["outcome"]["merge"], "fast_forward");
    assert_eq!(payload["branch"], "main");
    assert_eq!(payload["query_name"], "branch merge");
    assert_eq!(payload["affected_nodes"], 0);
    assert_eq!(payload["affected_edges"], 0);
    assert!(
        payload["commit"]["graph_commit_id"].as_str().is_some(),
        "a publishing merge reports the target's head: {payload}"
    );

    let deleted = output_success(embedded("mutate", &graph).arg("-e").arg("branch delete b0"));
    assert_eq!(
        stdout_string(&deleted),
        "deleted branch b0\n",
        "a local target needs no --yes for the delete"
    );
    let listed = output_success(
        embedded("query", &graph)
            .arg("-e")
            .arg("branch list")
            .arg("--json"),
    );
    assert_eq!(listed_names(&listed), ["main"]);

    let created = output_success(
        embedded("mutate", &graph)
            .arg("--as")
            .arg("act-stmt")
            .arg("-e")
            .arg("branch create b1 from main")
            .arg("--json"),
    );
    let payload = parse_stdout_json(&created);
    assert_eq!(payload["outcome"]["kind"], "created");
    assert_eq!(payload["outcome"]["from"], "main");
    assert_eq!(payload["outcome"]["name"], "b1");
    assert_eq!(payload["branch"], "b1");
    assert_eq!(payload["query_name"], "branch create");
    assert_eq!(
        payload["actor_id"], "act-stmt",
        "--as attributes the actor as on any embedded write"
    );
    assert_eq!(payload["commit"], Value::Null);
    let deleted = output_success(
        embedded("mutate", &graph)
            .arg("--as")
            .arg("act-stmt")
            .arg("-e")
            .arg("branch delete b1"),
    );
    assert_eq!(
        stdout_string(&deleted),
        "deleted branch b1\nactor_id: act-stmt\n"
    );
}

#[test]
fn branch_list_statement_renders_in_every_text_format() {
    let (_temp, graph) = loaded_graph();
    output_success(embedded("mutate", &graph).arg("-e").arg("branch create b0"));

    let render = |format: &str| {
        stdout_string(&output_success(
            embedded("query", &graph)
                .arg("-e")
                .arg("branch list")
                .arg("--format")
                .arg(format),
        ))
    };

    let table = render("table");
    assert!(
        table.starts_with("2 rows via branch list\n"),
        "a null-null target drops the `from ...` clause: {table}"
    );
    assert!(table.contains("name") && table.contains("b0") && table.contains("main"));

    let kv = render("kv");
    assert_eq!(
        kv,
        "2 rows via branch list\nrow 1\nname: b0\n\nrow 2\nname: main\n"
    );

    assert_eq!(render("csv"), "name\nb0\nmain\n");

    let jsonl = render("jsonl");
    let mut lines = jsonl.lines();
    let metadata: Value = serde_json::from_str(lines.next().unwrap()).unwrap();
    assert_eq!(metadata["kind"], "metadata");
    assert_eq!(metadata["query_name"], "branch list");
    assert_eq!(metadata["target"]["branch"], Value::Null);
    assert_eq!(metadata["target"]["snapshot"], Value::Null);
    assert_eq!(metadata["row_count"], 2);
    let rows: Vec<Value> = lines
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(
        rows,
        [
            serde_json::json!({"name": "b0"}),
            serde_json::json!({"name": "main"})
        ]
    );

    let json: Value = serde_json::from_str(&render("json")).unwrap();
    assert_eq!(json["query_name"], "branch list");
    assert_eq!(json["target"]["branch"], Value::Null);
    assert_eq!(json["target"]["snapshot"], Value::Null);
    assert_eq!(json["columns"], serde_json::json!(["name"]));
    assert_eq!(json["row_count"], 2);
    assert_eq!(json["graph_commit_id"], Value::Null);
    assert_eq!(
        json["rows"],
        serde_json::json!([{"name": "b0"}, {"name": "main"}])
    );
}

#[test]
fn branch_statements_remote_round_trip_and_delete_needs_consent() {
    let (_cluster, server) = served_graph();

    let created = output_success(served("mutate", &server).arg("-e").arg("branch create b0"));
    assert_eq!(stdout_string(&created), "created branch b0 from main\n");

    let table = stdout_string(&output_success(
        served("query", &server)
            .arg("-e")
            .arg("branch list")
            .arg("--format")
            .arg("table"),
    ));
    assert!(
        table.starts_with("2 rows via branch list\n") && table.contains("b0"),
        "table: {table}"
    );
    let listed = output_success(
        served("query", &server)
            .arg("-e")
            .arg("branch list")
            .arg("--json"),
    );
    let payload = parse_stdout_json(&listed);
    assert_eq!(payload["target"]["branch"], Value::Null);
    assert_eq!(payload["target"]["snapshot"], Value::Null);
    assert_eq!(listed_names(&listed), ["b0", "main"]);

    let merged = output_success(
        served("mutate", &server)
            .arg("-e")
            .arg("branch merge b0 into main"),
    );
    assert_eq!(
        stdout_string(&merged),
        "merged b0 into main: already_up_to_date\n"
    );

    let refused = output_failure(served("mutate", &server).arg("-e").arg("branch delete b0"));
    let stderr = stderr_string(&refused);
    assert!(
        stderr.contains("refusing destructive `branch delete` against non-local target")
            && stderr.contains("pass --yes to confirm"),
        "a served target is non-local: the statement takes the verb's consent step; got: {stderr}"
    );
    let refused_json = output_failure(
        served("mutate", &server)
            .arg("-e")
            .arg("branch delete b0")
            .arg("--json"),
    );
    assert!(
        stderr_string(&refused_json).contains("pass --yes to confirm"),
        "--json fails closed too"
    );
    let listed = output_success(
        served("query", &server)
            .arg("-e")
            .arg("branch list")
            .arg("--json"),
    );
    assert_eq!(
        listed_names(&listed),
        ["b0", "main"],
        "a refused delete changes nothing"
    );

    let deleted = output_success(
        served("mutate", &server)
            .arg("-e")
            .arg("branch delete b0")
            .arg("--yes"),
    );
    assert_eq!(stdout_string(&deleted), "deleted branch b0\n");
    let listed = output_success(
        served("query", &server)
            .arg("-e")
            .arg("branch list")
            .arg("--json"),
    );
    assert_eq!(listed_names(&listed), ["main"]);

    let created = output_success(
        served("mutate", &server)
            .arg("-e")
            .arg("branch create b1")
            .arg("--json"),
    );
    let payload = parse_stdout_json(&created);
    assert_eq!(payload["outcome"]["kind"], "created");
    assert_eq!(payload["outcome"]["from"], "main");
    assert_eq!(payload["outcome"]["name"], "b1");
    assert_eq!(payload["query_name"], "branch create");
    assert_eq!(payload["affected_nodes"], 0);
    assert_eq!(payload["commit"], Value::Null);

    diverge_alice(&|| served("mutate", &server));
    let conflicted = served("mutate", &server)
        .arg("-e")
        .arg("branch merge feature into main")
        .output()
        .unwrap();
    assert_eq!(conflicted.status.code(), Some(1));
    let stderr = stderr_string(&conflicted);
    assert!(
        stderr.contains("merge conflicts: ") && stderr.contains("Alice"),
        "served conflict names the conflicting entity; got: {stderr}"
    );
}

#[test]
fn branch_statement_local_refusals_happen_before_any_round_trip() {
    let unreachable = "http://127.0.0.1:9";
    let temp = tempdir().unwrap();
    let params_file = temp.path().join("params.json");
    std::fs::write(&params_file, "{}").unwrap();
    let params_file = params_file.to_str().unwrap();

    let refusal = |args: &[&str]| -> String {
        let mut command = cli();
        command
            .args(args)
            .arg("--server")
            .arg(unreachable)
            .arg("--graph")
            .arg("g");
        stderr_string(&output_failure(&mut command))
    };

    const TARGET: &str = "a branch statement names its branches itself; drop the request target";
    const NAME_OR_PARAMS: &str = "a branch statement takes no name and no parameters";
    const PRECONDITION: &str = "a branch statement takes no commit precondition";
    const CONTROL_WRITE_AT_QUERY: &str =
        "statement 'branch create' is a control write; use POST /mutate";
    const READ_AT_MUTATE: &str = "statement 'branch list' is a read; use POST /query";

    const WRONG_DOOR: &str = "wrong door";
    const DOOR_BEFORE_ENVELOPE: &str = "the door is checked before the envelope, as on the server";
    const QUERY_ENVELOPE: &str = "`query`: request target, then name or params";
    const MUTATE_ENVELOPE: &str =
        "`mutate`: request target, then name or params, then precondition";
    const ENVELOPE_BEFORE_CONSENT: &str = "the envelope is refused before the delete consent step";

    let cases: &[(&[&str], &str, &str)] = &[
        (
            &["query", "-e", "branch create b0"],
            CONTROL_WRITE_AT_QUERY,
            WRONG_DOOR,
        ),
        (
            &["query", "-e", "branch delete b0"],
            "statement 'branch delete' is a control write; use POST /mutate",
            WRONG_DOOR,
        ),
        (
            &["query", "-e", "branch merge b0 into main"],
            "statement 'branch merge' is a control write; use POST /mutate",
            WRONG_DOOR,
        ),
        (&["mutate", "-e", "branch list"], READ_AT_MUTATE, WRONG_DOOR),
        (
            &["query", "-e", "branch create b0", "--branch", "main"],
            CONTROL_WRITE_AT_QUERY,
            DOOR_BEFORE_ENVELOPE,
        ),
        (
            &["mutate", "-e", "branch list", "--branch", "main"],
            READ_AT_MUTATE,
            DOOR_BEFORE_ENVELOPE,
        ),
        (
            &["query", "-e", "branch list", "--branch", "main"],
            TARGET,
            QUERY_ENVELOPE,
        ),
        (
            &["query", "-e", "branch list", "--snapshot", "s1"],
            TARGET,
            QUERY_ENVELOPE,
        ),
        (
            &["query", "which", "-e", "branch list"],
            NAME_OR_PARAMS,
            QUERY_ENVELOPE,
        ),
        (
            &["query", "-e", "branch list", "--params", "{}"],
            NAME_OR_PARAMS,
            QUERY_ENVELOPE,
        ),
        (
            &["query", "-e", "branch list", "--params-file", params_file],
            NAME_OR_PARAMS,
            QUERY_ENVELOPE,
        ),
        (
            &["query", "which", "-e", "branch list", "--branch", "main"],
            TARGET,
            QUERY_ENVELOPE,
        ),
        (
            &["mutate", "-e", "branch create b0", "--branch", "main"],
            TARGET,
            MUTATE_ENVELOPE,
        ),
        (
            &["mutate", "which", "-e", "branch create b0"],
            NAME_OR_PARAMS,
            MUTATE_ENVELOPE,
        ),
        (
            &["mutate", "-e", "branch create b0", "--params", "{}"],
            NAME_OR_PARAMS,
            MUTATE_ENVELOPE,
        ),
        (
            &[
                "mutate",
                "-e",
                "branch create b0",
                "--params-file",
                params_file,
            ],
            NAME_OR_PARAMS,
            MUTATE_ENVELOPE,
        ),
        (
            &["mutate", "-e", "branch create b0", "--if-commit", "01HEAD"],
            PRECONDITION,
            MUTATE_ENVELOPE,
        ),
        (
            &[
                "mutate",
                "-e",
                "branch create b0",
                "--branch",
                "main",
                "--if-commit",
                "01HEAD",
            ],
            TARGET,
            MUTATE_ENVELOPE,
        ),
        (
            &[
                "mutate",
                "which",
                "-e",
                "branch create b0",
                "--if-commit",
                "01HEAD",
            ],
            NAME_OR_PARAMS,
            MUTATE_ENVELOPE,
        ),
        (
            &["mutate", "-e", "branch delete b0", "--branch", "main"],
            TARGET,
            ENVELOPE_BEFORE_CONSENT,
        ),
    ];
    for (args, expected, rule) in cases {
        let stderr = refusal(args);
        assert!(
            stderr.contains(expected),
            "{args:?} ({rule}): expected {expected:?}; got: {stderr}"
        );
        assert!(
            !stderr.contains("error sending request") && !stderr.contains("Connection refused"),
            "{args:?}: the refusal must not follow a round trip (nothing listens on \
             {unreachable}); got: {stderr}"
        );
    }

    let absent = temp.path().join("absent.omni");
    let stderr = stderr_string(&output_failure(
        embedded("query", &absent)
            .arg("-e")
            .arg("branch list")
            .arg("--branch")
            .arg("main"),
    ));
    assert!(
        stderr.contains(TARGET),
        "the embedded arm refuses the same way: {stderr}"
    );
    let stderr = stderr_string(&output_failure(
        embedded("mutate", &absent).arg("-e").arg("branch list"),
    ));
    assert!(
        stderr.contains(READ_AT_MUTATE),
        "the embedded arm refuses the same way: {stderr}"
    );
    assert!(
        !absent.exists(),
        "a refusal never opens or creates the store"
    );
}

#[test]
fn a_source_this_cli_cannot_parse_is_sent_to_the_server_verbatim() {
    let unreachable = "http://127.0.0.1:9";
    let sent_verbatim = |verb: &str| -> String {
        let mut command = cli();
        command
            .arg(verb)
            .arg("-e")
            .arg("not gq at all {")
            .arg("--server")
            .arg(unreachable)
            .arg("--graph")
            .arg("g");
        stderr_string(&output_failure(&mut command))
    };
    for verb in ["query", "mutate"] {
        let stderr = sent_verbatim(verb);
        assert!(
            stderr.contains("error sending request") || stderr.contains("Connection refused"),
            "{verb}: a source this CLI's grammar cannot parse is the server's to judge, so it \
             goes over the wire rather than failing locally (an older CLI never gates a newer \
             server's grammar); got: {stderr}"
        );
    }
}

/// `main` and `feature` set Alice's age to different values, so merging
/// `feature` into `main` conflicts.
fn diverge_alice(mutate: &dyn Fn() -> assert_cmd::Command) {
    output_success(mutate().arg("-e").arg("branch create feature"));
    output_success(
        mutate()
            .arg("-e")
            .arg(SET_AGE)
            .arg("--params")
            .arg(r#"{"name":"Alice","age":31}"#),
    );
    output_success(
        mutate()
            .arg("--branch")
            .arg("feature")
            .arg("-e")
            .arg(SET_AGE)
            .arg("--params")
            .arg(r#"{"name":"Alice","age":32}"#),
    );
}

#[test]
fn branch_merge_statement_conflict_exits_1_with_the_engine_message() {
    let (_temp, graph) = loaded_graph();
    diverge_alice(&|| embedded("mutate", &graph));
    let conflicted = embedded("mutate", &graph)
        .arg("-e")
        .arg("branch merge feature into main")
        .output()
        .unwrap();
    assert_eq!(conflicted.status.code(), Some(1));
    let stderr = stderr_string(&conflicted);
    assert!(
        stderr.contains("merge conflicts: "),
        "embedded conflict renders as the engine error; got: {stderr}"
    );
    assert!(
        stderr.contains("Alice"),
        "names the conflicting entity: {stderr}"
    );
    assert_eq!(
        stdout_string(&conflicted),
        "",
        "a conflict prints no outcome"
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

// Legacy `omnigraph.yaml` `aliases:` invoked via the `--alias` flag were
// removed in RFC-011 D4 — operator aliases now live under `omnigraph alias
// <name>` (the happy path is covered by system_local's operator-alias e2e).
// The legacy file-alias path has no CLI entry point.

#[test]
fn alias_flag_is_removed_from_query() {
    // RFC-011 D4: `--alias` no longer exists on query/mutate; use `alias <name>`.
    let output = output_failure(cli().arg("query").arg("--alias").arg("who"));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("unexpected argument") && stderr.contains("--alias"),
        "expected clap to reject --alias on query; got: {stderr}"
    );
}

#[test]
fn alias_unknown_name_errors_listing_defined() {
    // Hermetic: an unknown alias fails before any network, listing defined ones.
    let home = tempdir().unwrap();
    std::fs::write(
        home.path().join("config.yaml"),
        "servers:\n  dev:\n    url: https://x\naliases:\n  who:\n    server: dev\n    query: find_person\n",
    )
    .unwrap();
    let output = output_failure(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .arg("alias")
            .arg("nope"),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("unknown alias 'nope'") && stderr.contains("who"),
        "expected an unknown-alias error listing defined aliases; got: {stderr}"
    );
}

#[test]
fn alias_rejects_global_scope_flags_that_the_binding_owns() {
    for (flag, value) in [
        ("--server", "dev"),
        ("--graph", "local"),
        ("--store", "file:///tmp/graph.omni"),
        ("--cluster", "."),
        ("--profile", "prod"),
        ("--as", "act-op"),
    ] {
        let output = output_failure(cli().arg(flag).arg(value).arg("alias").arg("who"));
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("`alias` uses the server, graph, and stored query")
                && stderr.contains(flag),
            "expected {flag} to be rejected by the alias binding guard; got: {stderr}"
        );
    }
}

#[test]
fn queries_list_with_store_flag_errors() {
    // `queries list` reads a cluster's applied state; a single-graph `--store`
    // address can never apply. Rejected loudly at the addressing guard (was:
    // silently ignored, then failed later asking for a cluster).
    let output = output_failure(
        cli()
            .arg("--store")
            .arg("file:///tmp/graph.omni")
            .arg("queries")
            .arg("list"),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("`queries list` is a cluster control command")
            && stderr
                .contains("--store addresses a single graph's storage directly and does not apply"),
        "expected the addressing-guard store rejection; got: {stderr}"
    );
}

#[test]
fn queries_list_with_as_flag_errors() {
    // Read-only control verbs (`queries`, `policy`, `cluster status`, …) never
    // read the actor; only `cluster apply`/`cluster approve` do. `--as` on a
    // non-attributing control verb must be a loud guard error, not a silently
    // dropped identity (PR #377 review follow-up).
    let output = output_failure(
        cli()
            .arg("--as")
            .arg("act-alice")
            .arg("queries")
            .arg("list"),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("`queries list` is a cluster control command")
            && stderr.contains("--as")
            && stderr.contains("does not apply"),
        "expected the addressing-guard --as rejection; got: {stderr}"
    );
}

#[test]
fn queries_and_policy_wrong_server_scope_points_at_cluster_scope() {
    let output = output_failure(cli().arg("--server").arg("prod").arg("queries").arg("list"));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("pass --cluster <dir|uri>") && !stderr.contains("pass --config <dir>"),
        "queries should point at --cluster, not --config; got: {stderr}"
    );

    let output = output_failure(
        cli()
            .arg("--server")
            .arg("prod")
            .arg("policy")
            .arg("validate"),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("pass --cluster <dir|uri>") && !stderr.contains("pass --config <dir>"),
        "policy should point at --cluster, not --config; got: {stderr}"
    );
}

// RFC-011: `queries validate`/`list` source the registry + schemas from a
// converged cluster's applied state (`--cluster <dir>`), not omnigraph.yaml.

/// Build a converged single-graph cluster (id `knowledge`) with one stored
/// query. `query_block` is the YAML under the graph's `queries:` key.
fn converged_cluster_with_query(
    query_file: &str,
    query_src: &str,
    query_block: &str,
) -> tempfile::TempDir {
    let temp = tempdir().unwrap();
    let dir = temp.path();
    std::fs::copy(fixture("test.pg"), dir.join("graph.pg")).unwrap();
    write_query_file(&dir.join(query_file), query_src);
    std::fs::write(
        dir.join("cluster.yaml"),
        format!(
            "version: 1\nmetadata:\n  name: sys\nstate:\n  backend: cluster\n  lock: true\n\
             graphs:\n  knowledge:\n    schema: ./graph.pg\n    queries:\n{query_block}"
        ),
    )
    .unwrap();
    output_success(cli().arg("cluster").arg("import").arg("--config").arg(dir));
    output_success(cli().arg("cluster").arg("apply").arg("--config").arg(dir));
    temp
}

#[test]
fn queries_validate_exits_zero_on_clean_registry() {
    let cluster = converged_cluster_with_query(
        "find_person.gq",
        "query find_person($name: String) { match { $p: Person { name: $name } } return { $p.age } }",
        "      find_person:\n        file: ./find_person.gq\n",
    );
    let output = output_success(
        cli()
            .arg("queries")
            .arg("validate")
            .arg("--cluster")
            .arg(cluster.path()),
    );
    let stdout = stdout_string(&output);
    assert!(stdout.contains("OK"), "stdout:\n{stdout}");
}

#[test]
fn cluster_import_rejects_a_type_broken_query() {
    // In the cluster model a stored query is type-checked at the cluster
    // boundary (import/apply), so a broken query can never reach the applied
    // state `queries validate` reads — the gate is upstream. `Widget` is not in
    // the fixture schema, so import must reject it, naming the query.
    let temp = tempdir().unwrap();
    let dir = temp.path();
    std::fs::copy(fixture("test.pg"), dir.join("graph.pg")).unwrap();
    write_query_file(
        &dir.join("ghost.gq"),
        "query ghost() { match { $w: Widget } return { $w.name } }",
    );
    std::fs::write(
        dir.join("cluster.yaml"),
        "version: 1\nmetadata:\n  name: sys\nstate:\n  backend: cluster\n  lock: true\n\
         graphs:\n  knowledge:\n    schema: ./graph.pg\n    queries:\n      ghost:\n        file: ./ghost.gq\n",
    )
    .unwrap();
    let output = output_failure(cli().arg("cluster").arg("import").arg("--config").arg(dir));
    let combined = format!(
        "{}{}",
        stdout_string(&output),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined.contains("ghost"),
        "cluster import must reject the broken query, naming it; got:\n{combined}"
    );
}

#[test]
fn queries_list_prints_registered_query() {
    let cluster = converged_cluster_with_query(
        "find_person.gq",
        "query find_person($name: String) { match { $p: Person { name: $name } } return { $p.age } }",
        "      find_person:\n        file: ./find_person.gq\n",
    );
    let output = output_success(
        cli()
            .arg("queries")
            .arg("list")
            .arg("--cluster")
            .arg(cluster.path()),
    );
    let stdout = stdout_string(&output);
    assert!(stdout.contains("find_person"), "stdout:\n{stdout}");
    assert!(
        stdout.contains("$name: String"),
        "list should show typed params; stdout:\n{stdout}"
    );
}

#[test]
fn queries_list_surfaces_description_and_instruction() {
    // `@description`/`@instruction` are the whole point of a stored query in a
    // catalog — they tell an agent/operator what it does and how to invoke it.
    // The CLI catalog must surface them in both human and --json output, to
    // match the HTTP `GET /queries` surface.
    let cluster = converged_cluster_with_query(
        "described.gq",
        "query described($name: String) \
            @description(\"Find a person by exact name.\") \
            @instruction(\"Use for exact lookups; prefer search for fuzzy matches.\") \
            { match { $p: Person { name: $name } } return { $p.age } }",
        "      described:\n        file: ./described.gq\n",
    );

    // Human output.
    let output = output_success(
        cli()
            .arg("queries")
            .arg("list")
            .arg("--cluster")
            .arg(cluster.path()),
    );
    let stdout = stdout_string(&output);
    assert!(
        stdout.contains("description: Find a person by exact name."),
        "human list must show @description; stdout:\n{stdout}"
    );
    assert!(
        stdout.contains("instruction: Use for exact lookups; prefer search for fuzzy matches."),
        "human list must show @instruction; stdout:\n{stdout}"
    );

    // --json output.
    let output = output_success(
        cli()
            .arg("queries")
            .arg("list")
            .arg("--cluster")
            .arg(cluster.path())
            .arg("--json"),
    );
    let body: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let entry = body["queries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|q| q["name"] == "described")
        .unwrap();
    assert_eq!(entry["description"], "Find a person by exact name.");
    assert_eq!(
        entry["instruction"],
        "Use for exact lookups; prefer search for fuzzy matches."
    );
}

#[test]
fn queries_list_indents_multiline_annotation_continuation() {
    // GQ string literals admit newlines, so a `@description`/`@instruction`
    // can be multiline. Human output must indent continuation lines to align
    // under the first rather than breaking back to the left margin.
    let cluster = converged_cluster_with_query(
        "multi.gq",
        "query multi($name: String) \
            @description(\"line one\\nline two\") \
            { match { $p: Person { name: $name } } return { $p.age } }",
        "      multi:\n        file: ./multi.gq\n",
    );
    let output = output_success(
        cli()
            .arg("queries")
            .arg("list")
            .arg("--cluster")
            .arg(cluster.path()),
    );
    let stdout = stdout_string(&output);
    // "    description: " is 17 chars wide; the continuation aligns under it.
    assert!(
        stdout.contains("    description: line one\n                 line two"),
        "multiline annotation must indent the continuation; stdout:\n{stdout}"
    );
}

#[test]
fn queries_list_omits_annotations_when_absent() {
    // The other half of the contract: a query that declares neither annotation
    // prints no extra lines and omits both JSON fields entirely. This keeps the
    // catalog clean rather than echoing empty `description:`/`instruction:`.
    let cluster = converged_cluster_with_query(
        "bare.gq",
        "query bare() { match { $p: Person } return { $p.name } }",
        "      bare:\n        file: ./bare.gq\n",
    );

    // Human output: the query is listed, but no annotation lines.
    let output = output_success(
        cli()
            .arg("queries")
            .arg("list")
            .arg("--cluster")
            .arg(cluster.path()),
    );
    let stdout = stdout_string(&output);
    assert!(stdout.contains("bare()"), "stdout:\n{stdout}");
    assert!(
        !stdout.contains("description:") && !stdout.contains("instruction:"),
        "a query without annotations prints no annotation lines; stdout:\n{stdout}"
    );

    // --json output: both fields omitted (not present as null).
    let output = output_success(
        cli()
            .arg("queries")
            .arg("list")
            .arg("--cluster")
            .arg(cluster.path())
            .arg("--json"),
    );
    let body: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let entry = body["queries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|q| q["name"] == "bare")
        .unwrap();
    assert!(
        entry.get("description").is_none() && entry.get("instruction").is_none(),
        "a query without annotations omits both JSON fields: {entry}"
    );
}

#[test]
fn queries_validate_requires_a_cluster() {
    // RFC-011: with no --cluster (and no cluster profile), the command errors
    // loudly rather than reading any omnigraph.yaml.
    let output = output_failure(cli().arg("queries").arg("validate"));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("needs a cluster") || stderr.contains("--cluster"),
        "queries validate must require a cluster; stderr:\n{stderr}"
    );
}

#[test]
fn queries_validate_graph_filter_selects_one_graph() {
    // A multi-graph cluster: validate scoped to `knowledge` type-checks only
    // that graph's registry, ignoring `engineering`'s.
    let temp = tempdir().unwrap();
    let dir = temp.path();
    write_multi_graph_cluster_fixture(dir);
    output_success(cli().arg("cluster").arg("import").arg("--config").arg(dir));
    output_success(cli().arg("cluster").arg("apply").arg("--config").arg(dir));
    let output = output_success(
        cli()
            .arg("queries")
            .arg("validate")
            .arg("--cluster")
            .arg(dir)
            .arg("--graph")
            .arg("knowledge"),
    );
    assert!(stdout_string(&output).contains("OK"));
}
