//! Data commands: load/read/change/branch/commit/export/snapshot/policy/embed/maintenance.
//! Moved verbatim from tests/cli.rs in the modularization.

use std::fs;

use assert_cmd::Command;
use serde_json::Value;
use tempfile::tempdir;

mod support;

use support::*;


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
fn optimize_json_succeeds_on_local_graph() {
    // Happy path for the resolve_local_uri swap (RFC-010 Slice 1): a positional
    // local path still resolves and runs embedded.
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    let output = output_success(cli().arg("optimize").arg("--json").arg(&graph));
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(payload["tables"].as_array().is_some());
}

#[test]
fn optimize_with_server_flag_errors_wrong_plane() {
    // RFC-010 Slice 1: --server is a data-plane addressing flag; on a
    // storage-plane verb the guard rejects it loudly (was: silently ignored).
    let output = output_failure(cli().arg("optimize").arg("--server").arg("prod"));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("`optimize` is a direct (storage-native) command")
            && stderr.contains("--server addresses a served graph and does not apply")
            && stderr.contains("Pass a storage URI, or --cluster <dir> --graph <id>."),
        "wrong-capability guard message not found; got: {stderr}"
    );
}

#[test]
fn wrong_address_guard_message_has_no_trailing_space() {
    // The remediation tail is empty for served-addressing capabilities, so a
    // misplaced --cluster on a data verb must not leave "… does not apply. "
    // with a dangling space (error text is observable contract). NO_COLOR keeps
    // the assertion off ANSI styling.
    let output = output_failure(
        cli()
            .env("NO_COLOR", "1")
            .arg("query")
            .arg("--cluster")
            .arg("./brain")
            .arg("-e")
            .arg("query q { Person { id } }"),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("and does not apply."),
        "expected the wrong-address message; got: {stderr}"
    );
    assert!(
        !stderr.contains("and does not apply. "),
        "trailing space after the message; got: {stderr}"
    );
}

#[test]
fn graph_flag_on_a_positional_uri_errors() {
    // RFC-011: `--graph` selects within a multi-graph scope (a server or
    // cluster). An explicit `--store <uri>` is already a single graph, so
    // pairing it with `--graph` is a loud error, not a silently-dropped flag.
    // (The guard lets `--graph` reach a data verb; the scope resolver rejects
    // it.)
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    let output = output_failure(
        cli()
            .arg("query")
            .arg("--store")
            .arg(&graph)
            .arg("--graph")
            .arg("knowledge")
            .arg("-e")
            .arg("query q { Person { id } }"),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("already a single graph"),
        "expected --graph-on-explicit-store rejection; got: {stderr}"
    );
}

#[test]
fn query_by_name_against_a_store_needs_a_server() {
    // RFC-011 D3: by-name (catalog) invocation is served-only — the catalog is
    // server-owned, so a bare `--store` has nothing to resolve the name
    // against. The ad-hoc lane (`-e`/`--query`) is the local alternative.
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    let output = output_failure(
        cli()
            .arg("query")
            .arg("find_people")
            .arg("--store")
            .arg(&graph),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("needs a server"),
        "expected a served-only by-name error; got: {stderr}"
    );
}

#[test]
fn optimize_with_remote_target_errors_storage_plane() {
    // RFC-010 Slice 1: a maintenance verb pointed at a remote URI fails loudly
    // and declaratively (was: whatever Omnigraph::open said about an https URI).
    let output = output_failure(cli().arg("optimize").arg("https://graph.example.invalid"));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("`optimize` is a direct (storage-native) command and needs direct storage access")
            && stderr.contains("remote server"),
        "direct remote-target message not found; got: {stderr}"
    );
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

#[test]
fn deprecated_read_and_change_subcommands_emit_warnings() {
    // Both subcommands require `--query`/`--query-string`, so invoking them
    // with no args will exit non-zero. That's fine -- we only care that the
    // deprecation warning is printed before the argument-required error.
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
fn query_lint_can_resolve_graph_from_store_scope() {
    // RFC-011: lint resolves its graph target through `--store` (the direct
    // scope), not omnigraph.yaml's cli.graph; the .gq path is plain cwd-relative.
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
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

    let output = output_success(
        cli()
            .arg("query")
            .arg("lint")
            .arg("--query")
            .arg(&query_path)
            .arg("--store")
            .arg(&graph)
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
    // RFC-010/011: the direct (storage-native) verbs share one declared message
    // (was: "query lint is only supported against local graph URIs …").
    assert!(
        stderr.contains("`lint` is a direct (storage-native) command and needs direct storage access")
            && stderr.contains("remote server"),
        "direct remote-target message not found; got: {stderr}"
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
        stderr.contains("lint requires --schema <schema.pg>")
            || stderr.contains("no graph addressed"),
        "expected a schema-or-graph-target requirement; got: {stderr}"
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
            .arg("--mode")
            .arg("overwrite")
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
            .arg("--store")
            .arg(&graph)
            .arg("--query")
            .arg(&queries)
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
fn read_via_store_flag_and_profile_match_positional_uri() {
    // RFC-011 Slice A: the new scope addressing (--store, and a --profile that
    // binds a store) drives a read identically to the legacy positional URI —
    // the scope layer is additive, not a behavior change.
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);
    let queries = fixture("test.gq");

    let read_rows = |cmd: &mut Command| -> Value {
        let output = output_success(
            cmd.arg("--query")
                .arg(&queries)
                .arg("get_person")
                .arg("--params")
                .arg(r#"{"name":"Alice"}"#)
                .arg("--json"),
        );
        serde_json::from_slice(&output.stdout).unwrap()
    };

    // Baseline: --store names the graph.
    let baseline = read_rows(cli().arg("query").arg("--store").arg(&graph));
    assert_eq!(baseline["rows"][0]["p.name"], "Alice");

    // --store names the same graph directly.
    let via_store = read_rows(cli().arg("query").arg("--store").arg(&graph));
    assert_eq!(via_store["rows"], baseline["rows"]);

    // A profile binding that store, selected with --profile (no positional).
    let home = temp.path().join("op-home");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(
        home.join("config.yaml"),
        format!(
            "profiles:\n  local:\n    store: '{}'\n",
            graph.to_string_lossy()
        ),
    )
    .unwrap();
    let via_profile = read_rows(
        cli()
            .env("OMNIGRAPH_HOME", &home)
            .arg("query")
            .arg("--profile")
            .arg("local"),
    );
    assert_eq!(via_profile["rows"], baseline["rows"]);
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

// RFC-011: `policy validate|test|explain` source the Cedar bundle from a
// converged cluster's applied policies (`--cluster <dir>` + `--graph <id>`),
// not omnigraph.yaml's policy.file.

#[test]
fn policy_validate_accepts_cluster_bundle() {
    let cluster = converged_loaded_cluster("knowledge", Some(POLICY_YAML));

    let output = output_success(
        cli()
            .arg("policy")
            .arg("validate")
            .arg("--cluster")
            .arg(cluster.path())
            .arg("--graph")
            .arg("knowledge"),
    );
    let stdout = stdout_string(&output);

    assert!(stdout.contains("policy valid:"));
    assert!(stdout.contains("[2 actors]"));
}

#[test]
fn policy_validate_fails_for_invalid_cluster_bundle() {
    // The cluster does not validate a policy bundle's internal rules, so an
    // applied-but-malformed bundle reaches `policy validate`, which compiles it
    // and surfaces the error (here: a duplicate rule id).
    let cluster = converged_loaded_cluster(
        "knowledge",
        Some(
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
        ),
    );

    let output = output_failure(
        cli()
            .arg("policy")
            .arg("validate")
            .arg("--cluster")
            .arg(cluster.path())
            .arg("--graph")
            .arg("knowledge"),
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("duplicate policy rule id"),
        "expected a duplicate-rule error; got: {stderr}"
    );
}

#[test]
fn policy_test_runs_declarative_cases_against_cluster_bundle() {
    let cluster = converged_loaded_cluster("knowledge", Some(POLICY_YAML));
    let tests = cluster.path().join("policy.tests.yaml");
    fs::write(&tests, POLICY_TESTS_YAML).unwrap();

    let output = output_success(
        cli()
            .arg("policy")
            .arg("test")
            .arg("--cluster")
            .arg(cluster.path())
            .arg("--graph")
            .arg("knowledge")
            .arg("--tests")
            .arg(&tests),
    );
    let stdout = stdout_string(&output);

    assert!(stdout.contains("policy tests passed: 2 cases"));
}

#[test]
fn policy_explain_reports_decision_and_matched_rule() {
    let cluster = converged_loaded_cluster("knowledge", Some(POLICY_YAML));

    let allow = output_success(
        cli()
            .arg("policy")
            .arg("explain")
            .arg("--cluster")
            .arg(cluster.path())
            .arg("--graph")
            .arg("knowledge")
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
            .arg("--cluster")
            .arg(cluster.path())
            .arg("--graph")
            .arg("knowledge")
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
fn read_resolves_uri_from_default_store_scope() {
    // RFC-011: a zero-flag read resolves its graph from `defaults.store` in the
    // operator config (the local-dev default scope) — no omnigraph.yaml.
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);
    let home = tempdir().unwrap();
    std::fs::write(
        home.path().join("config.yaml"),
        format!("defaults:\n  store: {}\n", graph.to_string_lossy()),
    )
    .unwrap();

    let output = output_success(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .arg("read")
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"Alice"}"#)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["row_count"], 1);
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
            .arg("--store")
            .arg(&graph)
            .arg("--query")
            .arg(fixture("test.gq"))
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

/// RFC-007 PR 1: the format cascade's operator hop — `defaults.output` in
/// ~/.omnigraph/config.yaml applies when nothing more specific is given,
/// and `--format` still wins over it.
#[test]
fn read_uses_operator_default_output_format() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);
    let operator_home = tempdir().unwrap();
    fs::write(
        operator_home.path().join("config.yaml"),
        "defaults:\n  output: csv\n",
    )
    .unwrap();

    let read = |extra: &[&str]| {
        let mut command = cli();
        command
            .env("OMNIGRAPH_HOME", operator_home.path())
            .arg("read")
            .arg("--store")
            .arg(&graph)
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"Alice"}"#);
        for arg in extra {
            command.arg(arg);
        }
        stdout_string(&output_success(&mut command))
    };

    let stdout = read(&[]);
    assert!(
        stdout.lines().next().unwrap().contains("p.name") && stdout.contains("Alice"),
        "operator defaults.output: csv applies with no --format: {stdout}"
    );
    let stdout = read(&["--format", "jsonl"]);
    assert!(
        stdout.starts_with('{'),
        "--format wins over the operator default: {stdout}"
    );
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
            .arg("--store")
            .arg(&graph)
            .arg("--query")
            .arg(fixture("test.gq"))
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
            .arg("--store")
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
            .arg("--store")
            .arg(&graph)
            .arg("--query")
            .arg(fixture("test.gq"))
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
fn change_resolves_uri_and_default_branch_from_store_scope() {
    // RFC-011: a mutate resolves its graph from `--store` and defaults the
    // branch to main (no omnigraph.yaml cli.graph / cli.branch).
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);
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
            .arg("--store")
            .arg(&graph)
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
            .arg("--store")
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
            .arg("--store")
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
fn positional_http_uri_on_a_data_verb_is_rejected() {
    // RFC-011: a `--store` http(s):// URL no longer dispatches to a remote
    // server — that requires `--server <url>`.
    let output = output_failure(
        cli()
            .arg("query")
            .arg("--store")
            .arg("http://127.0.0.1:1")
            .arg("-e")
            .arg("query q() { match { $p: Person { } } return { $p } }"),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("must be addressed with `--server <url>`"),
        "expected store-remote rejection; got: {stderr}"
    );
}

#[test]
fn as_on_a_served_write_is_rejected() {
    // RFC-011: a served write resolves the actor from the bearer token, so --as
    // cannot set identity. It errors while building the remote client — before
    // any HTTP call, so no server is needed.
    let output = output_failure(
        cli()
            .arg("mutate")
            .arg("--server")
            .arg("http://127.0.0.1:1")
            .arg("--as")
            .arg("act-nope")
            .arg("-e")
            .arg("query add($name: String) { insert Person { name: $name } }")
            .arg("--params")
            .arg(r#"{"name":"X"}"#),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("`--as` is not allowed on a served write"),
        "expected --as-served rejection; got: {stderr}"
    );
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
            .arg("--store")
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
            .arg("--store")
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
            .arg("--store")
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

    let output = output_failure(cli().arg("read").arg("--store").arg(&repo).arg("-e").arg(""));
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

// ── RFC-011 Decision 9: write diagnostics + non-local destructive-confirm ──

#[test]
fn write_echoes_resolved_target_to_stderr() {
    // Every write echoes its resolved target + access path to stderr; --json
    // (stdout) is unaffected. A local load → "(direct, local)".
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    let data = fixture("test.jsonl");
    let output = output_success(
        cli()
            .arg("load")
            .arg("--mode")
            .arg("append")
            .arg("--data")
            .arg(&data)
            .arg(&graph)
            .arg("--json"),
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("omnigraph load →") && stderr.contains("(direct, local)"),
        "missing write-target echo; stderr: {stderr}"
    );
    // stdout still parses as JSON — the echo went to stderr.
    let _: Value = serde_json::from_slice(&output.stdout).unwrap();
}

#[test]
fn quiet_suppresses_the_write_target_echo() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    let data = fixture("test.jsonl");
    let output = output_success(
        cli()
            .arg("--quiet")
            .arg("load")
            .arg("--mode")
            .arg("append")
            .arg("--data")
            .arg(&data)
            .arg(&graph),
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        !stderr.contains("omnigraph load →"),
        "--quiet should suppress the echo; stderr: {stderr}"
    );
}

#[test]
fn branch_delete_against_non_local_scope_refuses_without_yes() {
    // No bucket needed: the confirm gate fires before the graph is opened.
    let output = output_failure(
        cli()
            .arg("branch")
            .arg("delete")
            .arg("--store")
            .arg("s3://fake-bucket/g.omni")
            .arg("feature")
            .arg("--json"),
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("refusing destructive `branch delete`") && stderr.contains("--yes"),
        "expected a non-local destructive refusal; stderr: {stderr}"
    );
}

#[test]
fn branch_delete_against_non_local_scope_passes_gate_with_yes() {
    // With --yes the gate is bypassed; the command then fails for an unrelated
    // reason (the fake bucket can't be opened), so the refusal must be ABSENT.
    let output = output_failure(
        cli()
            .arg("branch")
            .arg("delete")
            .arg("--store")
            .arg("s3://fake-bucket/g.omni")
            .arg("feature")
            .arg("--yes")
            .arg("--json"),
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        !stderr.contains("refusing destructive"),
        "--yes should bypass the confirm gate; stderr: {stderr}"
    );
}

#[test]
fn overwrite_load_against_non_local_scope_refuses_without_yes() {
    let output = output_failure(
        cli()
            .arg("load")
            .arg("--mode")
            .arg("overwrite")
            .arg("--data")
            .arg(fixture("test.jsonl"))
            .arg("--store")
            .arg("s3://fake-bucket/g.omni")
            .arg("--json"),
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("refusing destructive `load --mode overwrite`"),
        "expected a non-local overwrite refusal; stderr: {stderr}"
    );
}

#[test]
fn cleanup_against_non_local_scope_refuses_without_yes() {
    // Past the --confirm preview gate, a non-local cleanup still needs --yes.
    let output = output_failure(
        cli()
            .arg("cleanup")
            .arg("--store")
            .arg("s3://fake-bucket/g.omni")
            .arg("--keep")
            .arg("5")
            .arg("--confirm")
            .arg("--json"),
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("refusing destructive `cleanup`"),
        "expected a non-local cleanup refusal; stderr: {stderr}"
    );
}

#[test]
fn cleanup_against_local_scope_executes_with_confirm() {
    // Local cleanup needs no --yes; --confirm alone executes (and echoes).
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);
    let output = output_success(
        cli()
            .arg("cleanup")
            .arg("--keep")
            .arg("1")
            .arg("--confirm")
            .arg(&graph)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(payload["tables"].as_array().is_some(), "{payload}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("omnigraph cleanup →"), "stderr: {stderr}");
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
fn branch_merge_delete_branch_deletes_source() {
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
    let feature_data = temp.path().join("feature-delete.jsonl");
    write_jsonl(
        &feature_data,
        r#"{"type":"Person","data":{"name":"Gwen","age":35}}"#,
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
            .arg("--delete-branch")
            .arg("--json"),
    );
    let merge_payload: Value = serde_json::from_slice(&merge_output.stdout).unwrap();
    assert_eq!(merge_payload["outcome"], "fast_forward");
    assert_eq!(merge_payload["branch_deleted"], true);
    assert!(merge_payload["branch_delete_error"].is_null());

    let list_output = output_success(
        cli()
            .arg("branch")
            .arg("list")
            .arg("--uri")
            .arg(&graph)
            .arg("--json"),
    );
    let list_payload: Value = serde_json::from_slice(&list_output.stdout).unwrap();
    assert_eq!(list_payload["branches"], serde_json::json!(["main"]));
}

#[test]
fn branch_merge_delete_branch_refusal_warns_and_exits_zero() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    for (from, name) in [("main", "feature"), ("feature", "feature-child")] {
        output_success(
            cli()
                .arg("branch")
                .arg("create")
                .arg("--uri")
                .arg(&graph)
                .arg("--from")
                .arg(from)
                .arg(name),
        );
    }

    // `feature` has a dependent descendant, so the post-merge deletion is
    // refused — the merge (already_up_to_date: deletion is still attempted)
    // must succeed with exit code 0 and a stderr warning.
    let merge_output = output_success(
        cli()
            .arg("branch")
            .arg("merge")
            .arg("--uri")
            .arg(&graph)
            .arg("feature")
            .arg("--delete-branch")
            .arg("--json"),
    );
    let merge_payload: Value = serde_json::from_slice(&merge_output.stdout).unwrap();
    assert_eq!(merge_payload["outcome"], "already_up_to_date");
    assert_eq!(merge_payload["branch_deleted"], false);
    assert!(
        merge_payload["branch_delete_error"]
            .as_str()
            .unwrap()
            .contains("feature-child")
    );
    let stderr = String::from_utf8_lossy(&merge_output.stderr);
    assert!(stderr.contains("could not delete branch 'feature'"));

    let list_output = output_success(
        cli()
            .arg("branch")
            .arg("list")
            .arg("--uri")
            .arg(&graph)
            .arg("--json"),
    );
    let list_payload: Value = serde_json::from_slice(&list_output.stdout).unwrap();
    assert_eq!(
        list_payload["branches"],
        serde_json::json!(["feature", "feature-child", "main"])
    );
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
    assert_eq!(
        payload["internal_schema_version"].as_u64().unwrap(),
        u64::from(omnigraph::db::manifest::INTERNAL_MANIFEST_SCHEMA_VERSION)
    );
    assert!(payload["tables"].as_array().unwrap().len() >= 4);
}

#[test]
fn snapshot_resolves_uri_from_store_scope() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    let output = output_success(
        cli()
            .arg("snapshot")
            .arg("--store")
            .arg(&graph)
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
    assert!(stdout.contains(&format!(
        "internal_schema_version: {}",
        omnigraph::db::manifest::INTERNAL_MANIFEST_SCHEMA_VERSION
    )));
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
            .arg("--mode")
            .arg("overwrite")
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

/// RFC-011 Decision 8: `profile list` / `profile show` inspect the operator
/// config's profiles read-only. Hermetic via OMNIGRAPH_HOME.
fn profile_home() -> tempfile::TempDir {
    let home = tempdir().unwrap();
    std::fs::write(
        home.path().join("config.yaml"),
        "operator:\n  actor: act-andrew\n\
         defaults:\n  output: json\n  server: prod\n  default_graph: knowledge\n\
         servers:\n  prod:\n    url: https://graph.example.com\n\
         clusters:\n  brain:\n    root: s3://acme/clusters/brain\n\
         profiles:\n\
         \x20 staging:\n    server: prod\n    default_graph: kb\n\
         \x20 brain-admin:\n    cluster: brain\n\
         \x20 localdev:\n    store: file:///data/dev.omni\n\
         \x20 broken:\n    server: a\n    store: b\n",
    )
    .unwrap();
    home
}

#[test]
fn profile_list_names_each_profile_with_its_binding_and_marks_active() {
    let home = profile_home();
    let out = output_success(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .env("OMNIGRAPH_PROFILE", "staging")
            .arg("profile")
            .arg("list"),
    );
    let stdout = stdout_string(&out);
    assert!(stdout.contains("staging (active)"), "{stdout}");
    assert!(stdout.contains("server: prod"), "{stdout}");
    assert!(stdout.contains("cluster: brain"), "{stdout}");
    assert!(stdout.contains("store: file:///data/dev.omni"), "{stdout}");
    // A malformed (two-scope) profile is reported, not a hard failure.
    assert!(stdout.contains("broken") && stdout.contains("invalid:"), "{stdout}");
}

#[test]
fn profile_list_json_shape() {
    let home = profile_home();
    let out = output_success(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .arg("profile")
            .arg("list")
            .arg("--json"),
    );
    let items: Value = serde_json::from_slice(&out.stdout).unwrap();
    let brain = items
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["name"] == "brain-admin")
        .unwrap();
    assert_eq!(brain["binding"], "cluster: brain");
    assert_eq!(brain["scope_kind"], "cluster");
    assert_eq!(brain["target"], "brain");
    assert_eq!(brain["valid"], true);
    assert!(brain["error"].is_null());
    assert_eq!(brain["active"], false);
    let broken = items
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["name"] == "broken")
        .unwrap();
    assert_eq!(broken["scope_kind"], "invalid");
    assert_eq!(broken["valid"], false);
    assert!(broken["target"].is_null());
    assert!(
        broken["error"]
            .as_str()
            .unwrap()
            .contains("profile 'broken'")
    );
}

#[test]
fn profile_show_resolves_named_scope_endpoints() {
    let home = profile_home();
    // A cluster profile resolves its root.
    let cluster = output_success(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .arg("profile")
            .arg("show")
            .arg("brain-admin"),
    );
    let cs = stdout_string(&cluster);
    assert!(cs.contains("scope:   cluster brain"), "{cs}");
    assert!(cs.contains("endpoint: s3://acme/clusters/brain"), "{cs}");

    // A store profile shows its URI as the endpoint.
    let store = output_success(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .arg("profile")
            .arg("show")
            .arg("localdev")
            .arg("--json"),
    );
    let detail: Value = serde_json::from_slice(&store.stdout).unwrap();
    assert_eq!(detail["scope_kind"], "store");
    assert_eq!(detail["endpoint"], "file:///data/dev.omni");
}

#[test]
fn profile_show_without_name_falls_back_to_flat_defaults() {
    let home = profile_home();
    let out = output_success(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .arg("profile")
            .arg("show")
            .arg("--json"),
    );
    let detail: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(detail["name"], "(defaults)");
    assert_eq!(detail["scope_kind"], "server");
    assert_eq!(detail["endpoint"], "https://graph.example.com");
    assert_eq!(detail["default_graph"], "knowledge");
}

#[test]
fn profile_show_without_name_uses_active_env_profile() {
    let home = profile_home();
    let out = output_success(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .env("OMNIGRAPH_PROFILE", "brain-admin")
            .arg("profile")
            .arg("show")
            .arg("--json"),
    );
    let detail: Value = serde_json::from_slice(&out.stdout).unwrap();
    // No name arg, but $OMNIGRAPH_PROFILE selects brain-admin (not the flat defaults).
    assert_eq!(detail["name"], "brain-admin");
    assert_eq!(detail["scope_kind"], "cluster");
    assert_eq!(detail["endpoint"], "s3://acme/clusters/brain");
    // output_format renders as the canonical lowercase value name.
    assert_eq!(detail["output_format"], "json");
}

#[test]
fn profile_show_unknown_name_errors() {
    let home = profile_home();
    let out = output_failure(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .arg("profile")
            .arg("show")
            .arg("nope"),
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("unknown profile 'nope'"), "{stderr}");
}
