//! Stored-query commands and alias resolution.
//! Moved verbatim from tests/cli.rs in the modularization.

use std::{
    fs,
    path::{Path, PathBuf},
};

use serde_json::Value;
use tempfile::{TempDir, tempdir};

use crate::support::*;

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
    let start = parse_stdout_json(&output_success(
        embedded("commit", &graph).args(["list", "--branch", "b0", "--json"]),
    ))["commits"][0]["graph_commit_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        stdout_string(&created),
        format!("created branch b0 from main at {start}\n")
    );

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
    let start = parse_stdout_json(&output_success(
        served("commit", &server).args(["list", "--branch", "b0", "--json"]),
    ))["commits"][0]["graph_commit_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        stdout_string(&created),
        format!("created branch b0 from main at {start}\n")
    );

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
    let refusal = parse_stdout_json(&refused_json);
    assert!(
        refusal["error"]
            .as_str()
            .unwrap()
            .contains("pass --yes to confirm"),
        "--json fails closed too: {refusal}"
    );
    assert_eq!(
        refusal["disposition"],
        serde_json::json!({
            "effects":"none", "action":"refresh"
        })
    );
    assert!(refusal.get("http_status").is_none());
    assert!(refused_json.stderr.is_empty());
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
    for_each_concurrently(cases.to_vec(), CASE_WORKERS, |(args, expected, rule)| {
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
    });

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
    use crate::support::managed_http::{IntentApiFixture, IntentReply};

    // After contract admission, remote source validation belongs to the server.
    // Local statement classification must not replace its error or alter input.
    const SOURCE: &str = "not gq at all {";
    let refusal = serde_json::json!({
        "error": "server rejected malformed source", "code": "bad_request",
    });
    for (verb, route, source_field) in [
        ("query", "/graphs/g/query", "query"),
        ("mutate", "/graphs/g/mutate", "query"),
    ] {
        let server = IntentApiFixture::graph(vec![IntentReply::json(400, refusal.clone())]);
        let output = output_failure(
            cli()
                .arg(verb)
                .arg("-e")
                .arg(SOURCE)
                .arg("--server")
                .arg(&server.origin)
                .arg("--graph")
                .arg("g")
                .arg("--json"),
        );
        let mut actual = parse_stdout_json(&output);
        // A read changes nothing; an untyped refusal of a write leaves its
        // effects unknown.
        assert_eq!(actual["http_status"], 400);
        assert_eq!(
            actual["disposition"],
            if verb == "mutate" {
                serde_json::json!({
                    "effects":"unknown", "action":"reconcile"
                })
            } else {
                serde_json::json!({"effects":"none", "action":"fix"})
            }
        );
        let fields = actual.as_object_mut().unwrap();
        fields.remove("http_status");
        fields.remove("disposition");
        assert_eq!(
            actual, refusal,
            "{verb} must preserve every original server field"
        );
        assert!(output.stderr.is_empty(), "{verb}: {output:?}");
        let requests = server.requests();
        assert_eq!(requests.len(), 2, "one discovery and one data request");
        assert_eq!(requests[0].method, "HEAD");
        assert_eq!(requests[0].path, "/healthz");
        assert_eq!(requests[1].method, "POST");
        assert_eq!(requests[1].path, route);
        assert_eq!(requests[1].body[source_field], SOURCE);
        assert_eq!(
            requests[1].headers[omnigraph_api_types::HTTP_API_CONTRACT_HEADER],
            omnigraph_api_types::HTTP_API_CONTRACT
        );
        server.assert_complete();
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

/// The one `show` row of a `--json` answer.
fn shown_row(output: &std::process::Output) -> Value {
    let payload = parse_stdout_json(output);
    assert_eq!(payload["query_name"], "show");
    assert_eq!(payload["row_count"], 1, "{payload}");
    payload["rows"][0].clone()
}

#[test]
fn set_flag_reaches_the_embedded_session_and_a_set_line_wins_over_it() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);

    let baseline = shown_row(&output_success(
        embedded("query", &graph)
            .arg("-e")
            .arg("show merge_lineage;")
            .arg("--json"),
    ));
    assert_ne!(
        baseline["value"], "off",
        "the probe value must differ from the baseline: {baseline}"
    );

    let row = shown_row(&output_success(
        embedded("query", &graph)
            .arg("-e")
            .arg("show merge_lineage;")
            .arg("--set")
            .arg("merge_lineage=off")
            .arg("--json"),
    ));
    assert_eq!(row["name"], "merge_lineage");
    assert_eq!(row["value"], "off");
    assert_eq!(
        row["source"], "request",
        "`--set` is the invocation's request, the docs' `request` source: {row}"
    );

    let row = shown_row(&output_success(
        embedded("query", &graph)
            .arg("-e")
            .arg("set merge_lineage = on;\nshow merge_lineage;")
            .arg("--set")
            .arg("merge_lineage=off")
            .arg("--json"),
    ));
    assert_eq!(
        row["value"], "on",
        "the source's set line applies after --set"
    );
    assert_eq!(row["source"], "file");

    let row = shown_row(&output_success(
        embedded("query", &graph)
            .arg("-e")
            .arg("show stage_write_concurrency;")
            .arg("--set")
            .arg("stage_write_concurrency=4")
            .arg("--json"),
    ));
    assert_eq!(row["scope"], "process");
    assert_eq!(row["value"], "4");
    assert_eq!(
        row["source"], "request",
        "the embedded CLI is the process, so --set carries a process row a \
         server would refuse: {row}"
    );

    let row = shown_row(&output_success(
        embedded("query", &graph)
            .arg("-e")
            .arg("reset merge_lineage;\nshow merge_lineage;")
            .arg("--set")
            .arg("merge_lineage=off")
            .arg("--json"),
    ));
    assert_eq!(
        (&row["value"], &row["source"]),
        (&baseline["value"], &baseline["source"]),
        "`reset` drops the --set value to the process baseline (the build's \
         default, or the environment's value), not to the value it replaced: {row}"
    );
}

#[test]
fn set_flag_refusals_happen_before_any_open_or_round_trip() {
    let temp = tempdir().unwrap();
    let absent = temp.path().join("absent.omni");
    let definition_cases = [
        (
            "merge_lineage",
            "--set takes NAME=VALUE, got 'merge_lineage'",
        ),
        (
            "merge_lineage=fast",
            "unknown value `fast` for setting `merge_lineage`",
        ),
    ];
    for (flag, expected) in definition_cases {
        let stderr = stderr_string(&output_failure(
            embedded("query", &absent)
                .arg("-e")
                .arg("show merge_lineage;")
                .arg("--set")
                .arg(flag),
        ));
        assert!(
            stderr.contains(expected),
            "--set {flag}: expected {expected:?}; got: {stderr}"
        );
    }
    let unreachable = "http://127.0.0.1:9";
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
    const PROCESS_SCOPE: &str = "setting `stage_write_concurrency` is a process setting; it is \
                                 read from the server's environment, not from a request";
    const STORED_QUERY: &str =
        "--set applies to an ad-hoc source (-e '<gq>' / --query <file>), not to a stored query";
    let remote_cases: &[(&[&str], &str)] = &[
        (
            &[
                "query",
                "-e",
                "show all;",
                "--set",
                "stage_write_concurrency=4",
            ],
            PROCESS_SCOPE,
        ),
        (
            &[
                "query",
                "-e",
                "branch list",
                "--set",
                "stage_write_concurrency=4",
            ],
            PROCESS_SCOPE,
        ),
        (
            &[
                "mutate",
                "-e",
                SET_AGE,
                "--set",
                "stage_write_concurrency=4",
            ],
            PROCESS_SCOPE,
        ),
        (
            &[
                "branch",
                "merge",
                "b0",
                "--into",
                "main",
                "--set",
                "stage_write_concurrency=4",
            ],
            PROCESS_SCOPE,
        ),
        (
            &["query", "reports", "--set", "merge_lineage=off"],
            STORED_QUERY,
        ),
        (
            &["mutate", "add_source", "--set", "merge_lineage=off"],
            STORED_QUERY,
        ),
    ];
    for (args, expected) in remote_cases {
        let stderr = refusal(args);
        assert!(
            stderr.contains(expected),
            "{args:?}: expected {expected:?}; got: {stderr}"
        );
        assert!(
            !stderr.contains("error sending request") && !stderr.contains("Connection refused"),
            "{args:?}: the refusal must not follow a round trip (nothing listens on \
             {unreachable}); got: {stderr}"
        );
    }
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
        "servers:\n  dev:\n    url: https://x\ndefaults:\n  output: json\naliases:\n  who:\n    server: dev\n    query: find_person\n",
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
    assert!(
        output.stdout.is_empty(),
        "an error before alias capture cannot inherit its output defaults: {output:?}"
    );
}

#[test]
#[cfg(unix)]
fn alias_setup_keeps_one_operator_config_across_params_file() {
    use std::fs::{self, OpenOptions};
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    use std::time::{Duration, Instant};

    use crate::support::managed_http::{IntentApiFixture, IntentReply};
    use serde_json::json;

    let cases = [
        ("csv", "json", &["--json"][..], false, "json"),
        ("csv", "json", &[][..], false, "csv"),
        ("json", "csv", &[][..], true, "json"),
        ("jsonl", "csv", &[][..], true, "jsonl"),
        ("json", "jsonl", &["--format", "table"][..], true, "human"),
    ];
    for (alias_format, default_format, flags, refused, expected_format) in cases {
        let home = tempdir().unwrap();
        let config = home.path().join("config.yaml");
        let replacement = home.path().join("replacement.yaml");
        let params_file = home.path().join("params.json");
        let trap = IntentApiFixture::graph(vec![]);
        let refusal = json!({"error": "captured alias denied", "code": "forbidden"});
        let response = if refused {
            IntentReply::json(403, refusal.clone())
        } else {
            IntentReply::json(
                200,
                json!({
                    "query_name": "find_person",
                    "target": {"branch": "main"},
                    "row_count": 1,
                    "rows": [{"p.name": "original-server"}]
                }),
            )
        };
        let server = IntentApiFixture::graph(vec![response]);
        let captured_config = format!(
            "servers:\n  prod:\n    url: '{}'\n  graph-owner:\n    url: '{}/graphs/knowledge'\ndefaults:\n  output: {default_format}\naliases:\n  who:\n    server: prod\n    graph: knowledge\n    query: find_person\n    args: [name, limit]\n    params: {{name: fixed, limit: 1, source: original-alias}}\n    format: {alias_format}\ncaptured_only: true\n",
            server.origin, server.origin
        );
        fs::write(&config, captured_config).unwrap();
        let replacement_config = format!(
            "servers:\n  prod:\n    url: '{}'\n  replacement:\n    url: '{}/graphs/knowledge'\naliases:\n  who:\n    server: prod\n    graph: replacement\n    query: replacement_query\n    params: {{source: replacement-alias}}\nreplacement_only: true\n",
            trap.origin, server.origin
        );
        fs::write(&replacement, &replacement_config).unwrap();
        assert!(
            std::process::Command::new("mkfifo")
                .arg(&params_file)
                .status()
                .unwrap()
                .success()
        );

        let mut command = assert_cmd::Command::from(cli_process());
        command
            .current_dir(home.path())
            .env("OMNIGRAPH_HOME", home.path())
            .env("OMNIGRAPH_TOKEN_PROD", "captured-root-token")
            .env("OMNIGRAPH_TOKEN_GRAPH_OWNER", "captured-graph-token")
            .env("OMNIGRAPH_TOKEN_REPLACEMENT", "replacement-token")
            .env_remove("OMNIGRAPH_BEARER_TOKEN")
            .env_remove("OMNIGRAPH_PROFILE")
            .args(["alias", "who", "positional-name", "2", "--params-file"])
            .arg(&params_file)
            .args(flags)
            .timeout(Duration::from_secs(15));
        let output = std::thread::scope(|scope| {
            // assert_cmd kills and reaps the child on timeout, including if the
            // barrier fails before a writer can release its parameter-file read.
            let child = scope.spawn(move || command.output().unwrap());
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut params = loop {
                match OpenOptions::new()
                    .write(true)
                    .custom_flags(libc::O_NONBLOCK)
                    .open(&params_file)
                {
                    Ok(params) => break params,
                    Err(error)
                        if error.raw_os_error() == Some(libc::ENXIO)
                            && Instant::now() < deadline =>
                    {
                        std::thread::sleep(Duration::from_millis(2));
                    }
                    Err(error) => panic!("CLI did not open its parameter file: {error}"),
                }
            };
            // Opening the FIFO writer succeeds only once the CLI has opened its
            // reader, after loading the alias. Readiness, not the polling delay,
            // orders replacement before execute_operator_alias can start.
            fs::rename(&replacement, &config).unwrap();
            params
                .write_all(br#"{"name":"explicit-name","extra":"file"}"#)
                .unwrap();
            drop(params);
            child.join().unwrap()
        });

        assert_eq!(fs::read_to_string(&config).unwrap(), replacement_config);
        assert!(
            trap.requests().is_empty(),
            "alias setup changed destination"
        );
        assert_eq!(
            output.status.code(),
            Some(if refused { 1 } else { 0 }),
            "{expected_format}: {output:?}"
        );
        let stdout = stdout_string(&output);
        if refused {
            if expected_format == "human" {
                assert!(output.stdout.is_empty(), "{output:?}");
                assert!(
                    stderr_string(&output).contains("captured alias denied"),
                    "{output:?}"
                );
            } else {
                // An alias is a read: its refusal reports the disposition a
                // `query` does, with no effects.
                let mut reported = parse_stdout_json(&output);
                assert_eq!(
                    reported["disposition"],
                    serde_json::json!({"effects":"none","action":"refresh"}),
                    "{output:?}"
                );
                assert_eq!(reported["http_status"], 403, "{output:?}");
                let reported = reported.as_object_mut().unwrap();
                reported.remove("disposition");
                reported.remove("http_status");
                assert_eq!(
                    serde_json::Value::Object(reported.clone()),
                    refusal,
                    "{output:?}"
                );
                assert!(
                    !stderr_string(&output).contains("captured alias denied"),
                    "{output:?}"
                );
                if expected_format == "jsonl" {
                    assert_eq!(stdout.lines().count(), 1, "{stdout}");
                }
            }
        } else if expected_format == "csv" {
            assert_eq!(stdout, "p.name\noriginal-server\n");
        } else {
            assert_eq!(
                parse_stdout_json(&output)["rows"][0]["p.name"],
                "original-server"
            );
        }
        trap.assert_complete();
        server.assert_complete();
        let requests = server.workflow_requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "POST");
        assert_eq!(requests[0].path, "/graphs/knowledge/queries/find_person");
        assert_eq!(
            requests[0].headers["authorization"],
            "Bearer captured-graph-token"
        );
        assert_eq!(
            requests[0].body,
            json!({
                "expect_mutation": false,
                "params": {
                    "name": "explicit-name", "limit": 2,
                    "source": "original-alias", "extra": "file"
                }
            })
        );
        let stderr = stderr_string(&output);
        assert_eq!(
            stderr.matches("unknown key `captured_only`").count(),
            1,
            "{stderr}"
        );
        // Success and failure presentation use the same config as alias dispatch.
        assert_eq!(
            stderr.matches("unknown key `replacement_only`").count(),
            0,
            "{stderr}"
        );
    }
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
    // read the actor; only actor-bound `cluster apply` does. `--as` on a
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
    let home = tempdir().unwrap();
    fs::write(home.path().join("config.yaml"), "servers: [\n").unwrap();
    let server = crate::support::managed_http::IntentApiFixture::graph(Vec::new());
    for args in [
        vec!["queries", "list"],
        vec!["queries", "validate"],
        vec!["policy", "validate"],
        vec!["policy", "test", "--tests", "missing-tests.yaml"],
        vec!["policy", "explain", "--actor", "actor", "--action", "read"],
    ] {
        let output = output_failure(
            cli()
                .current_dir(home.path())
                .env("OMNIGRAPH_HOME", home.path())
                .args(["--server", &server.origin])
                .args(&args),
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("pass --cluster <dir|uri>") && !stderr.contains("pass --config <dir>"),
            "{args:?} should point at --cluster, not --config; got: {stderr}"
        );
        assert!(
            !stderr.contains("could not parse operator config"),
            "{stderr}"
        );
        assert!(output.stdout.is_empty(), "{args:?}: {output:?}");
        assert!(
            server.requests().is_empty(),
            "{args:?} must refuse before HTTP"
        );
        assert!(!home.path().join("__cluster").exists());
        assert!(!home.path().join("graphs").exists());
    }
    server.assert_complete();
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
    apply_cluster_fixture(dir);
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
fn cluster_apply_rejects_a_broken_query_naming_it_and_where() {
    // In the cluster model a stored query is checked at the cluster boundary
    // (apply), so a broken query can never reach the applied state
    // `queries validate` reads — the gate is upstream. `Widget` is not in the
    // fixture schema, so apply must reject `ghost`, naming it; `broken` does
    // not parse, and the human report also says where, from the diagnostic.
    let cases: [(&str, &str, &[&str]); 2] = [
        (
            "ghost",
            "query ghost() { match { $w: Widget } return { $w.name } }",
            &["ghost"],
        ),
        (
            "broken",
            "query broken() { match { $p: Person $p.age > } return { $p.name } }",
            &[
                "broken: parse error: expected operand",
                "  --> line 1, column 46",
            ],
        ),
    ];
    for (name, source, expected) in cases {
        let temp = tempdir().unwrap();
        let dir = temp.path();
        std::fs::copy(fixture("test.pg"), dir.join("graph.pg")).unwrap();
        write_query_file(&dir.join(format!("{name}.gq")), source);
        std::fs::write(
            dir.join("cluster.yaml"),
            format!(
                "version: 1\nmetadata:\n  name: sys\nstate:\n  backend: cluster\n  lock: true\n\
                 graphs:\n  knowledge:\n    schema: ./graph.pg\n    queries:\n      {name}:\n        file: ./{name}.gq\n"
            ),
        )
        .unwrap();
        let output = output_failure(cli().arg("cluster").arg("apply").arg("--config").arg(dir));
        let combined = format!(
            "{}{}",
            stdout_string(&output),
            String::from_utf8_lossy(&output.stderr)
        );
        for needle in expected {
            assert!(
                combined.contains(needle),
                "cluster apply must reject `{name}` with {needle:?}; got:\n{combined}"
            );
        }
    }
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

    // A cluster-root target addresses it too, named or as `defaults.target`,
    // and as a complete address it excludes `--cluster`.
    let home = tempfile::tempdir().unwrap();
    fs::write(
        home.path().join("config.yaml"),
        format!(
            "target_config_version: 1\ndefaults:\n  target: ops\ntargets:\n  ops:\n    kind: direct\n    scope: root\n    uri: '{}'\n  \
             remote:\n    kind: server\n    endpoint: https://graphs.example\n    auth: anonymous\n\
             servers:\n  prod:\n    url: https://prod.example\n\
             profiles:\n  ambient:\n    server: prod\n  admin:\n    cluster: '{}'\n",
            cluster.path().display(),
            cluster.path().join("elsewhere").display(),
        ),
    )
    .unwrap();
    for args in [&["--target", "ops"][..], &[][..]] {
        let output = output_success(
            cli()
                .env("OMNIGRAPH_HOME", home.path())
                .args(["queries", "list"])
                .args(args),
        );
        assert!(stdout_string(&output).contains("find_person"), "{args:?}");
    }
    let refused = output_failure(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .args(["queries", "list", "--target", "ops", "--cluster"])
            .arg(cluster.path()),
    );
    assert!(
        stderr_string(&refused).contains("mutually exclusive"),
        "{refused:?}"
    );
    let refused = output_failure(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .args(["queries", "list", "--target", "remote"]),
    );
    assert!(
        stderr_string(&refused).contains("not a cluster root"),
        "{refused:?}"
    );

    // The cluster is selected by the rules every command uses. An ambient
    // profile beside `defaults.target` is a conflict, not a silent winner.
    let refused = output_failure(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .env("OMNIGRAPH_PROFILE", "admin")
            .args(["queries", "list"]),
    );
    assert!(
        stderr_string(&refused).contains("OMNIGRAPH_PROFILE"),
        "{refused:?}"
    );
    // An explicit profile that names no cluster refuses; it never falls
    // through to the default target.
    let refused = output_failure(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .env_remove("OMNIGRAPH_PROFILE")
            .args(["queries", "list", "--profile", "ambient"]),
    );
    let stderr = stderr_string(&refused);
    assert!(
        stderr.contains("profile 'ambient' is not a cluster root"),
        "{refused:?}"
    );
    assert!(!stderr.contains("find_person"), "{refused:?}");
    // An explicit cluster profile wins over the default target.
    let refused = output_failure(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .env_remove("OMNIGRAPH_PROFILE")
            .args(["queries", "list", "--profile", "admin"]),
    );
    assert!(stderr_string(&refused).contains("elsewhere"), "{refused:?}");
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
    apply_cluster_fixture(dir);
    let state_path = dir.join("__cluster/state.json");
    let before = fs::read(&state_path).unwrap();
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

    let listed = parse_stdout_json(&output_success(
        cli().args(["queries", "list", "--cluster"]).arg(dir).args([
            "--graph",
            "knowledge",
            "--json",
        ]),
    ));
    assert_eq!(listed["queries"].as_array().unwrap().len(), 1);
    assert_eq!(listed["queries"][0]["name"], "find_person");

    let missing = output_failure(
        cli()
            .args(["queries", "validate", "--cluster"])
            .arg(dir)
            .args(["--graph", "missing", "--json"]),
    );
    assert!(missing.stdout.is_empty());
    assert!(stderr_string(&missing).contains("graph `missing` is not applied"));
    assert_eq!(fs::read(&state_path).unwrap(), before);
    assert!(!dir.join("__cluster/lock.json").exists());
}

/// RFC 0047's diagnostics contract at the CLI: a refused query reports its
/// code, position, expectation and one fix in every output format, with no
/// colour and no backtrace, on both transports.
#[test]
fn a_query_without_its_parameter_list_reports_q002_in_every_output_format() {
    let (_temp, graph) = loaded_graph();
    let refused = "query name { match { $p: Person } return { $p.name } }";
    let suggestion = serde_json::json!({
        "applicability": "machine_applicable",
        "edits": [{"start": 10, "end": 10, "replacement": "()"}]
    });

    let json = parse_stdout_json(&output_failure(
        embedded("query", &graph)
            .arg("-e")
            .arg(refused)
            .arg("--json"),
    ));
    assert_eq!(
        json["error"],
        "parse error: expected `(`: a query declares its parameters even when it has none"
    );
    assert_eq!(json["diagnostic"]["code"], "Q002");
    assert_eq!(json["diagnostic"]["fix"], "query name()");
    assert_eq!(json["diagnostic"]["position"]["line"], 1);
    assert_eq!(json["diagnostic"]["position"]["column"], 11);
    assert_eq!(json["diagnostic"]["suggestion"], suggestion);
    assert!(
        json.get("code").is_none(),
        "an embedded refusal has no HTTP code: {json}"
    );

    let jsonl = output_failure(
        embedded("query", &graph)
            .arg("-e")
            .arg(refused)
            .arg("--format")
            .arg("jsonl"),
    );
    let stdout = stdout_string(&jsonl);
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(
        lines.len(),
        1,
        "jsonl carries the error as one line: {stdout:?}"
    );
    let line: Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(line["diagnostic"]["code"], "Q002");
    assert_eq!(line["diagnostic"]["suggestion"], suggestion);

    let human = output_failure(embedded("query", &graph).arg("-e").arg(refused));
    let stderr = stderr_string(&human);
    assert!(
        stderr.contains("error[Q002]: parse error: expected `(`"),
        "{stderr}"
    );
    assert!(stderr.contains("--> line 1, column 11"), "{stderr}");
    assert!(stderr.contains("fix: query name()"), "{stderr}");
    assert!(!stderr.contains("\u{1b}["), "no colour codes: {stderr:?}");
    assert!(
        !stderr.contains("Backtrace") && !stderr.contains("Location:"),
        "no backtrace footer: {stderr}"
    );
    assert!(
        stdout_string(&human).is_empty(),
        "the human lane writes nothing to stdout"
    );

    let operator_home = tempdir().unwrap();
    let cases: &[(&str, &[&str], &str)] = &[
        ("json", &[], "json"),
        ("jsonl", &[], "jsonl"),
        ("json", &["--format", "table"], "human"),
        ("jsonl", &["--format", "csv"], "human"),
        ("csv", &["--json"], "json"),
        ("csv", &["--format", "jsonl"], "jsonl"),
    ];
    for &(default_format, flags, expected_format) in cases {
        std::fs::write(
            operator_home.path().join("config.yaml"),
            format!("defaults:\n  output: {default_format}\n"),
        )
        .unwrap();
        let output = output_failure(
            embedded("query", &graph)
                .env("OMNIGRAPH_HOME", operator_home.path())
                .args(["-e", refused])
                .args(flags),
        );
        assert_eq!(output.status.code(), Some(1), "{output:?}");
        if expected_format == "human" {
            assert!(output.stdout.is_empty(), "{output:?}");
            let stderr = stderr_string(&output);
            assert!(stderr.contains("error[Q002]:"), "{stderr}");
            assert!(stderr.contains("fix: query name()"), "{stderr}");
        } else {
            assert_eq!(
                parse_stdout_json(&output),
                json,
                "{default_format} {flags:?}"
            );
            assert!(output.stderr.is_empty(), "{output:?}");
            if expected_format == "jsonl" {
                assert_eq!(stdout_string(&output).lines().count(), 1, "{output:?}");
            }
        }
    }

    let (_cluster, server) = served_graph();
    let json = parse_stdout_json(&output_failure(
        served("query", &server)
            .arg("-e")
            .arg(refused)
            .arg("--json"),
    ));
    assert_eq!(json["code"], "bad_request");
    assert_eq!(json["diagnostic"]["code"], "Q002");
    assert_eq!(json["diagnostic"]["suggestion"], suggestion);
    assert_eq!(json["diagnostic"]["fix"], "query name()");
    let served_human = output_failure(served("query", &server).arg("-e").arg(refused));
    let stderr = stderr_string(&served_human);
    assert!(stderr.contains("error[Q002]:"), "{stderr}");
    assert!(stderr.contains("fix: query name()"), "{stderr}");
}
