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

// RFC-011: `queries validate`/`list` source the registry + schemas from a
// converged cluster's applied state (`--cluster <dir>`), not omnigraph.yaml.

/// Build a converged single-graph cluster (id `knowledge`) with one stored
/// query. `query_block` is the YAML under the graph's `queries:` key.
fn converged_cluster_with_query(query_file: &str, query_src: &str, query_block: &str) -> tempfile::TempDir {
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
