//! Stored-query commands and alias resolution.
//! Moved verbatim from tests/cli.rs in the modularization.


use serde_json::Value;
use tempfile::tempdir;

mod support;

use support::*;


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
    output_success(
        cli()
            .arg("load")
            .arg("--mode")
            .arg("overwrite")
            .arg("--data")
            .arg(&data)
            .arg(&graph),
    );
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
        stderr.contains("local") && stderr.contains("set `cli.graph`"),
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
fn queries_list_unknown_cli_graph_errors() {
    // `queries list` opens no graph URI, so unknown-graph validation can't ride
    // along on URI resolution the way it does for every other command. An
    // unknown `cli.graph` selection must still error (naming the graph) instead
    // of silently falling back to the top-level registry and showing the wrong
    // (or empty) catalog. (`--target` was removed; `cli.graph` drives selection.)
    let graph = SystemGraph::loaded();
    graph.write_query(
        "find_person.gq",
        "query find_person($name: String) { match { $p: Person { name: $name } } return { $p.age } }",
    );
    let config = graph.write_config(
        "omnigraph.yaml",
        &format!(
            "graphs:\n  local:\n    uri: '{}'\n    queries:\n      find_person:\n        file: ./find_person.gq\ncli:\n  graph: nonexistent\npolicy: {{}}\n",
            graph.path().to_string_lossy().replace('\'', "''"),
        ),
    );
    let output = output_failure(cli().arg("queries").arg("list").arg("--config").arg(&config));
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
