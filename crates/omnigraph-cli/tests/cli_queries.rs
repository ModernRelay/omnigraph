//! Stored-query commands and alias resolution.
//! Moved verbatim from tests/cli.rs in the modularization.


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
