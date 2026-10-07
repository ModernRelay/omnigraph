//! RFC-009 Phase 1 — the embedded/remote parity referee.
//!
//! For every CLI verb with an `is_remote` fork, run the identical
//! invocation against (a) the local graph directly and (b) a spawned
//! server on a twin copy of the same graph. Actor-bearing operations use the
//! SAME actor on both arms (local `--as act-parity`; remote bearer token
//! resolving to `act-parity`); read-only Blob commands reject `--as`.
//! Scrub the declared-volatile allowlist
//! (`support::scrub_volatile` — ids, wall-clock, transport locations);
//! everything else must match exactly.
//!
//! This test PINS behavior; it does not idealize it. Genuine divergences
//! discovered here are recorded in `KNOWN_DIVERGENCES` below (and filed),
//! never silently repaired — repairs are Phase 3's job, gated by this
//! referee staying green through the refactor.

use tempfile::TempDir;

#[path = "support/http_bench.rs"]
mod http_bench;
#[path = "support/http_perf_layout.rs"]
mod http_perf_layout;
#[path = "support/http_soak.rs"]
mod http_soak;
mod support;
use support::*;

/// Divergences between the arms that exist today, pinned as expectations.
/// Removing an entry requires the corresponding behavior change to be a
/// deliberate, release-noted decision (RFC-009 Compatibility).
const KNOWN_DIVERGENCES: &[&str] = &[
    // populated by the rows below as they are written
];

/// One matched setup per row: twin graphs + the parity Cedar bundle on the
/// served arm. The local (`--store`) arm carries no policy (RFC-011); the
/// bundle is permissive for `act-parity`, so the arms still agree.
struct Parity {
    _temp: TempDir,
    local: std::path::PathBuf,
    server: TestServer,
    blob_external_uri: Option<String>,
}

fn parity() -> Parity {
    let (temp, local, remote) = twin_graphs();
    // RFC-011 cluster-only: the remote arm is served from a converged
    // cluster directory (one graph, id `parity`), seeded with the same
    // fixture data as the local twin.
    let cluster_dir = parity_configs(temp.path(), &local, &remote);
    let server = spawn_server_with_cluster_env(
        &cluster_dir,
        &[(
            "OMNIGRAPH_SERVER_BEARER_TOKENS_JSON",
            r#"{"act-parity":"parity-tok"}"#,
        )],
    );
    Parity {
        _temp: temp,
        local,
        server,
        blob_external_uri: None,
    }
}

fn blob_parity() -> Parity {
    let temp = tempfile::tempdir().unwrap();
    let local = temp.path().join("blob-local.omni");
    let (cluster_dir, blob_external_uri) = blob_parity_config(temp.path(), &local);
    let server = spawn_server_with_cluster_env(
        &cluster_dir,
        &[(
            "OMNIGRAPH_SERVER_BEARER_TOKENS_JSON",
            r#"{"act-parity":"parity-tok"}"#,
        )],
    );
    Parity {
        _temp: temp,
        local,
        server,
        blob_external_uri: Some(blob_external_uri),
    }
}

impl Parity {
    fn run(&self, args: &[&str]) -> (std::process::Output, std::process::Output) {
        run_both(&self.local, &self.server.base_url, args)
    }
}

fn assert_parity(verb: &str, local: &std::process::Output, remote: &std::process::Output) {
    assert_eq!(
        local.status.code(),
        remote.status.code(),
        "{verb}: exit codes diverge\nlocal: {local:?}\nremote: {remote:?}"
    );
    if local.status.success() {
        let local_json = scrubbed_json(local);
        let remote_json = scrubbed_json(remote);
        assert_eq!(
            local_json, remote_json,
            "{verb}: scrubbed JSON diverges (left=local, right=remote)"
        );
    }
}

/// Write receipts name the exact commit produced by each independently-run
/// arm, so their commit identities and manifest lineage cannot match. Assert
/// that both real outputs carry a complete receipt, then normalize only those
/// receipt-local values before applying the ordinary parity comparison.
fn assert_write_parity(verb: &str, local: &std::process::Output, remote: &std::process::Output) {
    assert_eq!(
        local.status.code(),
        remote.status.code(),
        "{verb}: exit codes diverge\nlocal: {local:?}\nremote: {remote:?}"
    );
    assert!(
        local.status.success(),
        "{verb}: local write failed: {local:?}"
    );

    let normalize_receipt = |arm: &str, output: &std::process::Output| {
        let mut payload = parse_stdout_json(output);
        let commit = payload["commit"]
            .as_object_mut()
            .unwrap_or_else(|| panic!("{verb}: {arm} output must carry a commit receipt"));
        assert!(
            commit["graph_commit_id"].as_str().is_some(),
            "{verb}: {arm} receipt must carry graph_commit_id"
        );
        assert!(
            commit["graph_manifest_version"].as_u64().is_some(),
            "{verb}: {arm} receipt must carry graph_manifest_version"
        );
        for key in [
            "graph_commit_id",
            "parent_commit_id",
            "merged_parent_commit_id",
        ] {
            if let Some(value) = commit.get_mut(key)
                && !value.is_null()
            {
                *value = serde_json::Value::String(format!("<volatile:{key}>"));
            }
        }
        scrub_volatile(&mut payload);
        payload
    };

    assert_eq!(
        normalize_receipt("local", local),
        normalize_receipt("remote", remote),
        "{verb}: normalized write JSON diverges (left=local, right=remote)"
    );
}

fn assert_blob_bytes_parity(
    case: &str,
    local: &std::process::Output,
    remote: &std::process::Output,
    expected: &[u8],
) {
    assert_eq!(
        local.status.code(),
        remote.status.code(),
        "{case}: exit codes diverge\nlocal: {local:?}\nremote: {remote:?}"
    );
    assert!(
        local.status.success(),
        "{case}: local arm failed: {local:?}"
    );
    assert!(
        remote.status.success(),
        "{case}: remote arm failed: {remote:?}"
    );
    assert_eq!(local.stdout, expected, "{case}: wrong local bytes");
    assert_eq!(remote.stdout, expected, "{case}: wrong remote bytes");
}

fn assert_blob_stat_parity(
    case: &str,
    local: &std::process::Output,
    remote: &std::process::Output,
) -> serde_json::Value {
    assert_eq!(
        local.status.code(),
        remote.status.code(),
        "{case}: exit codes diverge\nlocal: {local:?}\nremote: {remote:?}"
    );
    assert!(
        local.status.success(),
        "{case}: local stat failed: {local:?}"
    );
    assert!(
        remote.status.success(),
        "{case}: remote stat failed: {remote:?}"
    );

    let local_json: serde_json::Value = serde_json::from_slice(&local.stdout).unwrap();
    let remote_json: serde_json::Value = serde_json::from_slice(&remote.stdout).unwrap();
    for (arm, value) in [("local", &local_json), ("remote", &remote_json)] {
        let resolved = value["target"]["resolved_snapshot"].as_str().unwrap();
        assert!(
            !resolved.is_empty(),
            "{case}: {arm} stat omitted its exact resolved target"
        );
    }

    assert_eq!(
        local_json, remote_json,
        "{case}: exact stat metadata diverges"
    );
    local_json
}

fn normalized_blob_error(output: &std::process::Output) -> &str {
    let stderr = std::str::from_utf8(&output.stderr).unwrap();
    stderr
        .lines()
        .find_map(|line| line.trim().strip_prefix("0: "))
        .unwrap_or_else(|| panic!("Blob failure omitted its primary diagnostic: {stderr}"))
}

#[test]
fn parity_query() {
    let p = parity();
    let query = fixture("test.gq");
    let (l, r) = p.run(&[
        "query",
        "--query",
        query.to_str().unwrap(),
        "get_person",
        "--params",
        r#"{"name":"Alice"}"#,
        "--json",
    ]);
    assert_parity("query", &l, &r);
}

#[test]
fn parity_schema_show() {
    let p = parity();
    let (l, r) = p.run(&["schema", "show", "--json"]);
    assert_parity("schema show", &l, &r);
}

#[test]
fn parity_snapshot() {
    let p = parity();
    let (l, r) = p.run(&["snapshot", "--json"]);
    assert_parity("snapshot", &l, &r);
}

#[test]
fn parity_branch_list() {
    let p = parity();
    let (l, r) = p.run(&["branch", "list", "--json"]);
    assert_parity("branch list", &l, &r);
}

#[test]
fn parity_commit_list() {
    let p = parity();
    let (l, r) = p.run(&["commit", "list", "--json"]);
    assert_parity("commit list", &l, &r);
}

#[test]
fn parity_commit_list_branch() {
    // Exercises the `--branch` fork: the remote arm sends `?branch=` while the
    // embedded arm passes the branch straight to the engine.
    let p = parity();
    let (l, r) = p.run(&["commit", "list", "--branch", "main", "--json"]);
    assert_parity("commit list --branch", &l, &r);
}

#[test]
fn parity_mutate() {
    let p = parity();
    let (l, r) = p.run(&[
        "mutate",
        "-e",
        "query add($name: String, $age: I32) { insert Person { name: $name, age: $age } }",
        "--params",
        r#"{"name":"Parity","age":7}"#,
        "--json",
    ]);
    assert_write_parity("mutate", &l, &r);

    let (l, r) = p.run(&[
        "mutate",
        "-e",
        "query no_match() { update Person set { age: 99 } where name = \"Nobody\" }",
        "--json",
    ]);
    assert_parity("no-op mutate", &l, &r);
    for (arm, output) in [("local", &l), ("remote", &r)] {
        let payload = parse_stdout_json(output);
        assert_eq!(payload["affected_nodes"], 0, "{arm} no-op affected rows");
        assert_eq!(
            payload["commit"],
            serde_json::Value::Null,
            "{arm} no-op mutation must not invent a commit"
        );
    }
}

#[test]
fn parity_branch_create_delete() {
    let p = parity();
    let (l, r) = p.run(&[
        "branch",
        "create",
        "--from",
        "main",
        "parity-branch",
        "--json",
    ]);
    assert_parity("branch create", &l, &r);
    // `branch delete` is destructive: the served (remote) arm is non-local and
    // requires consent (RFC-011 Decision 9), so the row passes `--yes` to test
    // the operation itself, not the safety gate. The local arm ignores `--yes`.
    let (l, r) = p.run(&["branch", "delete", "parity-branch", "--yes", "--json"]);
    assert_parity("branch delete", &l, &r);
}

#[test]
fn parity_branch_merge() {
    let p = parity();
    let (l, r) = p.run(&["branch", "create", "--from", "main", "feature", "--json"]);
    assert_parity("branch create (merge setup)", &l, &r);
    let (l, r) = p.run(&[
        "mutate",
        "--branch",
        "feature",
        "-e",
        "query add() { insert Person { name: \"Receipt\", age: 31 } }",
        "--json",
    ]);
    assert_write_parity("merge source write", &l, &r);
    let (l, r) = p.run(&["branch", "merge", " feature ", "--into", " main ", "--json"]);
    assert_write_parity("branch merge own publication", &l, &r);
    for output in [&l, &r] {
        let payload = parse_stdout_json(output);
        assert_eq!(payload["source"], "feature");
        assert_eq!(payload["target"], "main");
        assert_eq!(payload["commit"]["graph_branch"], serde_json::Value::Null);
    }
    let (l, r) = p.run(&["branch", "merge", "feature", "--into", "main", "--json"]);
    assert_parity("branch merge", &l, &r);
    assert_eq!(parse_stdout_json(&l)["outcome"], "already_up_to_date");
    assert_eq!(
        parse_stdout_json(&l).get("commit"),
        Some(&serde_json::Value::Null)
    );
    // `--delete-branch` composes merge + delete at each arm's own boundary
    // (embedded: two engine calls; remote: the server handler) — this row is
    // the referee that keeps the two composition sites from drifting.
    let (l, r) = p.run(&["branch", "create", "--from", "main", "feature2", "--json"]);
    assert_parity("branch create (delete-branch setup)", &l, &r);
    let (l, r) = p.run(&[
        "branch",
        "merge",
        " feature2 ",
        "--into",
        " main ",
        "--delete-branch",
        "--json",
    ]);
    assert_parity("branch merge --delete-branch", &l, &r);
    for output in [&l, &r] {
        assert!(output.status.success(), "{output:?}");
        let payload = parse_stdout_json(output);
        assert_eq!(payload["source"], "feature2");
        assert_eq!(payload["target"], "main");
        assert_eq!(payload["branch_deleted"], true);
    }
    let (l, r) = p.run(&["branch", "list", "--json"]);
    assert_parity("branch list (post delete-branch)", &l, &r);
    assert!(
        !parse_stdout_json(&l)["branches"]
            .as_array()
            .unwrap()
            .iter()
            .any(|branch| branch == "feature2")
    );

    let (l, r) = p.run(&["branch", "create", "retained", "--json"]);
    assert_parity("branch create (deletion refusal setup)", &l, &r);
    let (l, r) = p.run(&[
        "mutate",
        "-e",
        "query add() { insert Person { name: \"Retained\", age: 32 } }",
        "--json",
    ]);
    assert_write_parity("refused deletion source write", &l, &r);
    let (l, r) = p.run(&[
        "branch",
        "merge",
        "main",
        "--into",
        "retained",
        "--delete-branch",
        "--json",
    ]);
    assert_write_parity("merge succeeds despite source deletion refusal", &l, &r);
    for output in [&l, &r] {
        let payload = parse_stdout_json(output);
        assert_eq!(payload["branch_deleted"], false);
        assert!(payload["branch_delete_error_details"]["code"].is_string());
        assert!(payload.get("branch_delete_error").is_none());
    }
    let (l, r) = p.run(&["branch", "list", "--json"]);
    assert_parity("branch list (source retained after refusal)", &l, &r);
    assert!(
        parse_stdout_json(&l)["branches"]
            .as_array()
            .unwrap()
            .iter()
            .any(|b| b == "main")
    );

    let before = p.run(&["commit", "list", "--json"]);
    for (source, target) in [(" ", "main"), ("retained", "\t")] {
        let (l, r) = p.run(&["branch", "merge", source, "--into", target]);
        for output in [&l, &r] {
            assert_eq!(output.status.code(), Some(1));
            assert!(
                String::from_utf8_lossy(&output.stderr)
                    .contains("branch merge source and target must not be empty"),
                "{output:?}"
            );
        }
    }
    let after = p.run(&["commit", "list", "--json"]);
    for (before, after) in [(&before.0, &after.0), (&before.1, &after.1)] {
        assert!(before.status.success() && after.status.success());
        assert_eq!(parse_stdout_json(before), parse_stdout_json(after));
    }
}

fn listed_statement_names(output: &std::process::Output) -> Vec<String> {
    parse_stdout_json(output)["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["name"].as_str().unwrap().to_string())
        .collect()
}

#[test]
fn parity_branch_statements() {
    let p = parity();
    let (l, r) = p.run(&["mutate", "-e", "branch create stmt", "--json"]);
    assert_parity("branch create statement", &l, &r);
    assert_eq!(parse_stdout_json(&l)["outcome"]["kind"], "created");

    let (l, r) = p.run(&["query", "-e", "branch list", "--json"]);
    assert_parity("branch list statement", &l, &r);
    assert_eq!(listed_statement_names(&l), ["main", "stmt"]);

    let (l, r) = p.run(&["mutate", "-e", "branch merge stmt into main", "--json"]);
    assert_parity("branch merge statement (already up to date)", &l, &r);
    assert_eq!(
        parse_stdout_json(&l)["outcome"]["merge"],
        "already_up_to_date"
    );

    let (l, r) = p.run(&[
        "mutate",
        "--branch",
        "stmt",
        "-e",
        "query add($name: String, $age: I32) { insert Person { name: $name, age: $age } }",
        "--params",
        r#"{"name":"Stmt","age":1}"#,
        "--json",
    ]);
    assert_write_parity("mutate on the statement branch", &l, &r);
    let (l, r) = p.run(&["mutate", "-e", "branch merge stmt into main", "--json"]);
    assert_write_parity(
        "branch merge statement (fast_forward: both arms report their own publication)",
        &l,
        &r,
    );
    assert_eq!(parse_stdout_json(&l)["outcome"]["merge"], "fast_forward");

    let (l, r) = p.run(&["mutate", "-e", "branch delete stmt", "--yes", "--json"]);
    assert_parity(
        "branch delete statement (--yes: the served arm is non-local)",
        &l,
        &r,
    );
    assert_eq!(parse_stdout_json(&l)["outcome"]["kind"], "deleted");
}

#[test]
fn parity_verb_and_statement_agree() {
    let p = parity();
    let (verb_l, verb_r) = p.run(&["branch", "create", "--from", "main", "via-verb", "--json"]);
    let (stmt_l, stmt_r) = p.run(&["mutate", "-e", "branch create via_stmt from main", "--json"]);
    for (arm, verb, stmt) in [("local", &verb_l, &stmt_l), ("remote", &verb_r, &stmt_r)] {
        let verb = parse_stdout_json(verb);
        let stmt = parse_stdout_json(stmt);
        assert_eq!(verb["from"], stmt["outcome"]["from"], "{arm}: create from");
        assert_eq!(verb["name"], "via-verb", "{arm}: verb name");
        assert_eq!(stmt["outcome"]["name"], "via_stmt", "{arm}: statement name");
        assert_eq!(verb["actor_id"], stmt["actor_id"], "{arm}: create actor");
    }

    let (verb_l, verb_r) = p.run(&["branch", "list", "--json"]);
    let (stmt_l, stmt_r) = p.run(&["query", "-e", "branch list", "--json"]);
    for (arm, verb, stmt) in [("local", &verb_l, &stmt_l), ("remote", &verb_r, &stmt_r)] {
        let verb: Vec<String> = parse_stdout_json(verb)["branches"]
            .as_array()
            .unwrap()
            .iter()
            .map(|name| name.as_str().unwrap().to_string())
            .collect();
        assert_eq!(verb, listed_statement_names(stmt), "{arm}: branch list");
        assert_eq!(
            verb,
            ["main", "via-verb", "via_stmt"],
            "{arm}: both branches listed"
        );
    }

    let (verb_l, verb_r) = p.run(&["branch", "merge", "via-verb", "--into", "main", "--json"]);
    let (stmt_l, stmt_r) = p.run(&["mutate", "-e", "branch merge via_stmt into main", "--json"]);
    for (arm, verb, stmt) in [("local", &verb_l, &stmt_l), ("remote", &verb_r, &stmt_r)] {
        let verb = parse_stdout_json(verb);
        let stmt = parse_stdout_json(stmt);
        assert_eq!(
            verb["target"], stmt["outcome"]["target"],
            "{arm}: merge target"
        );
        assert_eq!(
            verb["outcome"], stmt["outcome"]["merge"],
            "{arm}: merge result"
        );
        assert_eq!(
            verb["outcome"], "already_up_to_date",
            "{arm}: nothing to merge"
        );
        assert_eq!(verb["actor_id"], stmt["actor_id"], "{arm}: merge actor");
    }

    let (verb_l, verb_r) = p.run(&["branch", "delete", "via-verb", "--yes", "--json"]);
    let (stmt_l, stmt_r) = p.run(&["mutate", "-e", "branch delete via_stmt", "--yes", "--json"]);
    for (arm, verb, stmt) in [("local", &verb_l, &stmt_l), ("remote", &verb_r, &stmt_r)] {
        let verb = parse_stdout_json(verb);
        let stmt = parse_stdout_json(stmt);
        assert_eq!(verb["name"], "via-verb", "{arm}: verb delete");
        assert_eq!(
            stmt["outcome"]["name"], "via_stmt",
            "{arm}: statement delete"
        );
        assert_eq!(verb["actor_id"], stmt["actor_id"], "{arm}: delete actor");
    }
    let (verb_l, _) = p.run(&["branch", "list", "--json"]);
    assert_eq!(
        parse_stdout_json(&verb_l)["branches"],
        serde_json::json!(["main"]),
        "both deletions took effect"
    );
}

#[test]
fn parity_load() {
    let p = parity();
    let data = p.local.parent().unwrap().join("rows.jsonl");
    std::fs::write(
        &data,
        "{\"type\":\"Person\",\"data\":{\"name\":\"Loaded\",\"age\":1}}\n",
    )
    .unwrap();
    let (l, r) = p.run(&[
        "load",
        "--mode",
        "merge",
        "--data",
        data.to_str().unwrap(),
        "--json",
    ]);
    assert_write_parity("load", &l, &r);

    // Canonical load is strict graph-batch syntax, but Overwrite uses Lance's
    // replacement transaction rather than RFC-023's keyed Append/Merge adapter.
    // Keep this one row above that adapter's ceiling so local and served CLI
    // routing cannot accidentally impose the keyed limit on bulk replacement.
    const KEYED_LIMIT: usize = 8192;
    let mut overwrite = String::with_capacity((KEYED_LIMIT + 1) * 64);
    for (name, age) in [("Alice", 30), ("Bob", 25), ("Charlie", 35), ("Diana", 28)] {
        overwrite.push_str(&format!(
            "{{\"type\":\"Person\",\"data\":{{\"name\":\"{name}\",\"age\":{age}}}}}\n"
        ));
    }
    for row in 0..(KEYED_LIMIT - 3) {
        overwrite.push_str(&format!(
            "{{\"type\":\"Person\",\"data\":{{\"name\":\"Bulk {row}\",\"age\":1}}}}\n"
        ));
    }
    std::fs::write(&data, overwrite).unwrap();
    let (l, r) = p.run(&[
        "load",
        "--mode",
        "overwrite",
        "--data",
        data.to_str().unwrap(),
        "--yes",
        "--json",
    ]);
    assert!(l.status.success(), "bulk Overwrite local arm failed: {l:?}");
    assert!(
        r.status.success(),
        "bulk Overwrite remote arm failed: {r:?}"
    );
    assert_write_parity("load --mode overwrite above keyed limit", &l, &r);

    // The engine creates --from's branch before parsing the load payload. A
    // later refusal must describe the whole invocation, never imply that the
    // branch was not created or that the load can be blindly replayed.
    std::fs::write(&data, "not valid graph NDJSON\n").unwrap();
    let (l, r) = p.run(&[
        "load",
        "--mode",
        "append",
        "--data",
        data.to_str().unwrap(),
        "--branch",
        "partial-load",
        "--from",
        "main",
        "--json",
    ]);
    for (arm, output) in [("local", &l), ("remote", &r)] {
        assert_eq!(output.status.code(), Some(1), "{arm}: {output:?}");
        let output = parse_stdout_json(output);
        assert_eq!(output["command_outcome"]["execution"], "unknown", "{arm}");
        assert_eq!(output["command_outcome"]["effects"], "unknown", "{arm}");
        assert_ne!(output["command_outcome"]["action"], "retry", "{arm}");
    }
    let (l, r) = p.run(&["branch", "list", "--json"]);
    assert_parity("compound load retained the created branch", &l, &r);
    for output in [&l, &r] {
        let payload = parse_stdout_json(output);
        assert!(
            payload["branches"]
                .as_array()
                .unwrap()
                .iter()
                .any(|name| name == "partial-load")
        );
    }
}

#[test]
fn parity_load_embedding_diagnostics() {
    let temp = tempfile::tempdir().unwrap();
    let local = temp.path().join("local.omni");
    let schema = temp.path().join("embeddings.pg");
    std::fs::write(
        &schema,
        format!(
            "{}\nnode Doc {{ slug: String @key body: String embedding: Vector(2)? @embed(body) }}\n",
            std::fs::read_to_string(fixture("test.pg")).unwrap(),
        ),
    )
    .unwrap();
    let cluster_dir = parity_configs_with_schema(temp.path(), &local, &schema);
    let server = spawn_server_with_cluster_env(
        &cluster_dir,
        &[(
            "OMNIGRAPH_SERVER_BEARER_TOKENS_JSON",
            r#"{"act-parity":"parity-tok"}"#,
        )],
    );
    let p = Parity {
        _temp: temp,
        local,
        server,
        blob_external_uri: None,
    };
    let data = p.local.parent().unwrap().join("embeddings.jsonl");
    std::fs::write(&data, concat!(
        r#"{"type":"Doc","data":{"slug":"omitted","body":"missing vector"}}"#,
        "\n",
        r#"{"type":"Doc","data":{"slug":"supplied","body":"keep vector","embedding":[0.25,0.75]}}"#,
    )).unwrap();
    {
        let verb = "load";
        for structured in [true, false] {
            let mut args = vec![
                verb,
                "--branch",
                "main",
                "--mode",
                "merge",
                "--data",
                data.to_str().unwrap(),
            ];
            if structured {
                args.push("--json");
            }
            let (local, remote) = p.run(&args);
            for (arm, output) in [("local", &local), ("remote", &remote)] {
                assert!(output.status.success(), "{verb} {arm}: {output:?}");
                if structured {
                    assert_eq!(
                        parse_stdout_json(output)["embedding_generation"],
                        "unsupported",
                        "{verb} {arm}"
                    );
                } else {
                    let human = String::from_utf8_lossy(&output.stdout);
                    assert!(
                        human.contains("Loads do not generate embeddings."),
                        "{verb} {arm}: {human}"
                    );
                    assert!(human.contains("omnigraph embed"), "{verb} {arm}: {human}");
                }
            }
            if structured {
                assert_write_parity("load embedding diagnostics", &local, &remote);
            }
        }
    }
    let (local, remote) = p.run(&[
        "query",
        "-e",
        "query docs() { match { $d: Doc } return { $d.slug, $d.embedding } order { $d.slug asc } }",
        "--json",
    ]);
    // Each arm has independently published commits; compare their contents,
    // not the intentionally different graph-commit identities.
    for output in [&local, &remote] {
        assert!(output.status.success(), "{output:?}");
        assert_eq!(
            parse_stdout_json(output)["rows"],
            serde_json::json!([
                {"d.slug": "omitted"},
                {"d.slug": "supplied", "d.embedding": [0.25, 0.75]},
            ])
        );
    }

    std::fs::write(
        &data,
        r#"{"type":"Person","data":{"name":"Plain","age":1}}"#,
    )
    .unwrap();
    {
        let verb = "load";
        let (local, remote) = p.run(&[
            verb,
            "--branch",
            "main",
            "--mode",
            "merge",
            "--data",
            data.to_str().unwrap(),
            "--json",
        ]);
        for output in [&local, &remote] {
            assert!(output.status.success(), "{verb}: {output:?}");
            assert_eq!(
                parse_stdout_json(output).get("embedding_generation"),
                Some(&serde_json::Value::Null)
            );
        }
    }
}

#[test]
fn parity_export() {
    let p = parity();
    let (l, r) = p.run(&["export"]);
    assert!(
        r.status.success(),
        "export remote arm failed: {r:?}\nserver stderr:\n{}",
        p.server.stderr()
    );
    // export emits a JSONL STREAM, not a single `--json` document, so the
    // scrubbed-single-doc `assert_parity` doesn't apply — compare line-wise.
    // The twin graphs are byte-copies of one loaded fixture, so rows carry
    // identical ids/versions and need no scrubbing; sort the lines so any
    // cross-arm row-ordering difference doesn't masquerade as a divergence.
    assert_eq!(
        l.status.code(),
        r.status.code(),
        "export: exit codes diverge\nlocal {l:?}\nremote {r:?}"
    );
    assert!(l.status.success(), "export local arm failed: {l:?}");
    let mut local_lines: Vec<&str> = std::str::from_utf8(&l.stdout).unwrap().lines().collect();
    let mut remote_lines: Vec<&str> = std::str::from_utf8(&r.stdout).unwrap().lines().collect();
    assert!(
        !local_lines.is_empty(),
        "export produced no rows — the parity check would be vacuous"
    );
    local_lines.sort_unstable();
    remote_lines.sort_unstable();
    assert_eq!(
        local_lines, remote_lines,
        "export: JSONL streams diverge (left=local, right=remote)"
    );

    #[cfg(unix)]
    assert_slow_export_and_baseline_complete(&p);
}

/// Exercise the actual Hyper/socket ownership boundary. A Tower body consumer
/// cannot reproduce a transport retaining yielded chunks behind a full socket.
#[cfg(unix)]
fn assert_slow_export_and_baseline_complete(p: &Parity) {
    use std::io::{Read, Write};
    use std::time::{Duration, Instant};

    let data = p._temp.path().join("slow-export.jsonl");
    let mut file = std::io::BufWriter::new(std::fs::File::create(&data).unwrap());
    for row in 0..4096 {
        serde_json::to_writer(
            &mut file,
            &serde_json::json!({
                "type": "Person",
                "data": {"name": format!("slow-{row:04}-{}", "x".repeat(2048)), "age": 12}
            }),
        )
        .unwrap();
        file.write_all(b"\n").unwrap();
    }
    file.flush().unwrap();
    let (local, remote) = p.run(&[
        "load",
        "--mode",
        "merge",
        "--data",
        data.to_str().unwrap(),
        "--json",
    ]);
    assert_write_parity("slow export fixture", &local, &remote);
    let expected = output_success(cli().args(["export", "--store", p.local.to_str().unwrap()]));
    assert!(
        expected.stdout.len() > 8 * 1024 * 1024,
        "fixture must exceed socket buffers"
    );

    let address = p.server.base_url.strip_prefix("http://").unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap();
    for route in ["export", "changes/baseline"] {
        let mut socket = runtime.block_on(async {
            let socket = tokio::net::TcpSocket::new_v4().unwrap();
            // Negotiate TCP with the small receive buffer. Shrinking it after
            // connect can throttle Linux loopback even after reads resume.
            socket.set_recv_buffer_size(16 * 1024).unwrap();
            socket
                .connect(address.parse().unwrap())
                .await
                .unwrap()
                .into_std()
                .unwrap()
        });
        socket.set_nonblocking(false).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(15)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(15)))
            .unwrap();
        let request = r#"{"branch":"main"}"#;
        write!(socket,
            "POST /graphs/parity/{route} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer parity-tok\r\n{}: {}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{request}",
            omnigraph_api_types::HTTP_API_CONTRACT_HEADER,
            omnigraph_api_types::HTTP_API_CONTRACT,
            request.len(),
        ).unwrap();
        let mut first = [0; 4096];
        let count = socket.read(&mut first).unwrap();
        assert!(count > 0);
        let mut wire = first[..count].to_vec();
        // A headers-only first read does not prove export production started.
        // Consume a little body data before holding the receive window closed.
        let first_body_deadline = Instant::now() + Duration::from_secs(15);
        while !wire
            .windows(4)
            .position(|part| part == b"\r\n\r\n")
            .is_some_and(|offset| wire.len() > offset + 4 + 64)
        {
            let remaining = first_body_deadline.saturating_duration_since(Instant::now());
            assert!(!remaining.is_zero(), "{route}: no response body arrived");
            socket.set_read_timeout(Some(remaining)).unwrap();
            let count = socket.read(&mut first).unwrap();
            assert!(count > 0, "{route}: response closed before body data");
            wire.extend_from_slice(&first[..count]);
            assert!(wire.len() < 64 * 1024, "{route}: invalid response headers");
        }
        // This is a protocol-deadline regression, not a throughput threshold:
        // socket backpressure must outlive the 250 ms admission timeout.
        std::thread::sleep(Duration::from_secs(2));
        let drain_started = Instant::now();
        let deadline = drain_started + Duration::from_secs(30);
        let mut buffer = [0; 64 * 1024];
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(!remaining.is_zero(), "{route}: response did not complete");
            socket.set_read_timeout(Some(remaining)).unwrap();
            let count = socket.read(&mut buffer).unwrap_or_else(|error| {
                panic!(
                    "{route}: response read failed after {:?} and {} bytes: {error}\nserver stderr:\n{}",
                    drain_started.elapsed(),
                    wire.len(),
                    p.server.stderr()
                )
            });
            if count == 0 {
                break;
            }
            wire.extend_from_slice(&buffer[..count]);
            assert!(
                wire.len() <= 64 * 1024 * 1024,
                "{route}: unexpected response growth"
            );
        }
        let header_end = wire
            .windows(4)
            .position(|part| part == b"\r\n\r\n")
            .unwrap()
            + 4;
        let headers = std::str::from_utf8(&wire[..header_end]).unwrap();
        assert!(headers.starts_with("HTTP/1.1 200"), "{route}: {headers}");
        assert!(
            headers
                .to_ascii_lowercase()
                .contains("transfer-encoding: chunked")
        );
        let mut remaining = &wire[header_end..];
        let mut body = Vec::new();
        loop {
            let end = remaining
                .windows(2)
                .position(|part| part == b"\r\n")
                .unwrap_or_else(|| {
                    panic!("{route}: truncated response without chunked terminator")
                });
            let count =
                usize::from_str_radix(std::str::from_utf8(&remaining[..end]).unwrap(), 16).unwrap();
            remaining = &remaining[end + 2..];
            if count == 0 {
                assert_eq!(remaining, b"\r\n", "{route}: invalid terminal chunk");
                break;
            }
            assert!(remaining.len() >= count + 2, "{route}: incomplete chunk");
            body.extend_from_slice(&remaining[..count]);
            assert_eq!(&remaining[count..count + 2], b"\r\n");
            remaining = &remaining[count + 2..];
        }
        if route == "changes/baseline" {
            assert!(
                body.starts_with(&expected.stdout),
                "baseline snapshot changed"
            );
            let terminal: serde_json::Value =
                serde_json::from_slice(&body[expected.stdout.len()..]).unwrap();
            assert!(terminal["baseline"]["resume_cursor"].as_str().is_some());
            assert!(
                terminal["baseline"]["snapshot_commit_id"]
                    .as_str()
                    .is_some()
            );
        } else {
            assert_eq!(body, expected.stdout, "slow export lost or changed rows");
        }
    }
}

#[test]
fn parity_blob_get_is_byte_exact_for_full_range_empty_edge_and_snapshot_reads() {
    let p = blob_parity();

    let (local, remote) = p.run(&["blob", "get", "node", "Document", "readme", "content"]);
    assert_blob_bytes_parity("blob get full", &local, &remote, BLOB_NODE_BYTES);

    let (local, remote) = p.run(&[
        "blob", "get", "node", "Document", "readme", "content", "--offset", "1", "--length", "4",
    ]);
    assert_blob_bytes_parity("blob get range", &local, &remote, &[1, 2, 3, 4]);

    let (local, remote) = p.run(&["blob", "get", "node", "Document", "empty", "content"]);
    assert_blob_bytes_parity("blob get valid empty", &local, &remote, &[]);

    let (local, remote) = p.run(&[
        "blob",
        "get",
        "edge",
        "Attachment",
        "attachment-1",
        "payload",
    ]);
    assert_blob_bytes_parity("blob get edge", &local, &remote, BLOB_EDGE_BYTES);

    let snapshot = resolved_snapshot_id(&p.local, "main");
    let (local, remote) = p.run(&[
        "blob",
        "get",
        "node",
        "Document",
        "readme",
        "content",
        "--snapshot",
        &snapshot,
    ]);
    assert_blob_bytes_parity("blob get snapshot", &local, &remote, BLOB_NODE_BYTES);
}

#[test]
fn parity_blob_stat_preserves_exact_managed_and_external_metadata() {
    let p = blob_parity();

    let (local, remote) = p.run(&[
        "blob", "stat", "node", "Document", "readme", "content", "--json",
    ]);
    let local_managed = assert_blob_stat_parity("managed Blob stat", &local, &remote);
    assert_eq!(local_managed["kind"], "managed");
    assert_eq!(local_managed["size"], BLOB_NODE_BYTES.len());
    assert!(
        local_managed["etag"].as_str().is_some_and(|etag| {
            etag.len() == 34 && etag.starts_with('"') && etag.ends_with('"')
        })
    );

    let (local, remote) = p.run(&[
        "blob", "stat", "node", "Document", "external", "content", "--json",
    ]);
    let local_external = assert_blob_stat_parity("external Blob stat", &local, &remote);
    assert_eq!(local_external["kind"], "external");
    assert_eq!(
        local_external["uri"],
        p.blob_external_uri.as_deref().unwrap()
    );
    assert!(local_external.get("size").is_none());
    assert!(local_external.get("etag").is_none());
}

#[test]
fn parity_blob_external_get_never_follows_the_redirect() {
    let p = blob_parity();
    let (local, remote) = p.run(&["blob", "get", "node", "Document", "external", "content"]);
    assert_eq!(
        local.status.code(),
        remote.status.code(),
        "external get exit codes diverge\nlocal: {local:?}\nremote: {remote:?}"
    );
    assert!(!local.status.success());
    assert!(local.stdout.is_empty());
    assert!(remote.stdout.is_empty());
    let external_uri = p.blob_external_uri.as_deref().unwrap();
    for (arm, output) in [("local", local), ("remote", remote)] {
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(external_uri), "{arm}: {stderr}");
        assert!(stderr.contains("blob stat"), "{arm}: {stderr}");
    }
}

#[test]
fn parity_blob_shared_failures_keep_exit_codes_aligned() {
    let p = blob_parity();
    for (case, args) in [
        (
            "zero range",
            vec![
                "blob", "get", "node", "Document", "readme", "content", "--length", "0",
            ],
        ),
        (
            "out of bounds range",
            vec![
                "blob", "get", "node", "Document", "readme", "content", "--offset", "7",
            ],
        ),
        (
            "null cell",
            vec![
                "blob", "stat", "node", "Document", "null", "content", "--json",
            ],
        ),
        (
            "missing row",
            vec![
                "blob", "stat", "node", "Document", "missing", "content", "--json",
            ],
        ),
        (
            "non-Blob property",
            vec![
                "blob", "stat", "node", "Document", "readme", "note", "--json",
            ],
        ),
    ] {
        let (local, remote) = p.run(&args);
        assert_eq!(
            local.status.code(),
            remote.status.code(),
            "{case}: exit codes diverge\nlocal: {local:?}\nremote: {remote:?}"
        );
        assert!(!local.status.success(), "{case}: both arms must fail");
        assert_eq!(
            normalized_blob_error(&local),
            normalized_blob_error(&remote),
            "{case}: normalized diagnostics diverge\nlocal: {}\nremote: {}",
            String::from_utf8_lossy(&local.stderr),
            String::from_utf8_lossy(&remote.stderr),
        );
    }
}

// ---- error parity: exit codes must match for shared failure cases ----

#[test]
fn parity_errors_share_exit_codes() {
    let p = parity();

    // unknown branch on merge
    let (l, r) = p.run(&[
        "branch",
        "merge",
        "no-such-branch",
        "--into",
        "main",
        "--json",
    ]);
    assert_eq!(
        (l.status.success(), r.status.success()),
        (false, false),
        "merge of unknown branch must fail on both arms\nlocal {l:?}\nremote {r:?}"
    );

    // unknown query name in the source
    let query = fixture("test.gq");
    let (l, r) = p.run(&[
        "query",
        "--query",
        query.to_str().unwrap(),
        "no_such_query",
        "--json",
    ]);
    assert_eq!(
        (l.status.success(), r.status.success()),
        (false, false),
        "unknown query name must fail on both arms\nlocal {l:?}\nremote {r:?}"
    );

    // Required parameters fail closed on both transports, including when the
    // parameter is consumed by an inline node-property match.
    let (l, r) = p.run(&[
        "query",
        "--query",
        query.to_str().unwrap(),
        "get_person",
        "--json",
    ]);
    assert_eq!(
        (l.status.success(), r.status.success()),
        (false, false),
        "unbound required param must fail on both arms\nlocal {l:?}\nremote {r:?}"
    );
}

// ---- documented exclusions (not bugs; the Phase 4 capability table) ----
//
// - `graphs list`: server-only today; becomes Both-capability when the
//   embedded arm enumerates the cluster catalog (RFC-009 open Q3, answered).
// - `init`, `optimize`, `repair`, `cleanup`, `cluster *`: storage-plane by
//   design (must work with the server down); Phase 4 declares this.
#[allow(dead_code)]
const EXCLUSIONS_DOCUMENTED: () = ();

#[test]
fn known_divergences_ledger_is_current() {
    // The ledger exists so removals are deliberate: an empty list with all
    // rows green means the arms agree everywhere the matrix looks.
    assert!(
        KNOWN_DIVERGENCES.is_empty(),
        "divergences are pinned: {KNOWN_DIVERGENCES:?}"
    );
}

// ─── Change surfaces ────────────────────────────────────────────────────────

/// The twin graphs share one copied history, so commit ids, opaque type ids,
/// cursors, and page tokens are identical across the arms: parity here is
/// byte-exact after the ordinary scrub.
fn fixture_load_commit(p: &Parity) -> String {
    let (l, _r) = p.run(&["commit", "list", "--json"]);
    let commits: serde_json::Value =
        serde_json::from_slice(&l.stdout).expect("commit list emits JSON");
    let commits = commits["commits"].as_array().unwrap();
    // The oldest commit WITH a parent is the fixture load — a non-empty diff.
    commits
        .iter()
        .rev()
        .find(|commit| !commit["parent_commit_id"].is_null())
        .expect("history has a non-genesis commit")["graph_commit_id"]
        .as_str()
        .unwrap()
        .to_string()
}

#[test]
fn parity_commit_changes() {
    let p = parity();
    let commit_id = fixture_load_commit(&p);
    // --limit 1 forces the client to walk page tokens internally on BOTH arms.
    let (l, r) = p.run(&["commit", "changes", &commit_id, "--limit", "1", "--json"]);
    assert_parity("commit changes", &l, &r);
}

#[test]
fn parity_commit_changes_filtered() {
    let p = parity();
    let commit_id = fixture_load_commit(&p);
    let (l, r) = p.run(&[
        "commit", "changes", &commit_id, "--kind", "node", "--op", "insert", "--json",
    ]);
    assert_parity("commit changes filtered", &l, &r);
}

#[test]
fn parity_changes_poll_beginning() {
    let p = parity();
    let (l, r) = p.run(&["changes", "poll", "--start", "beginning", "--json"]);
    assert_parity("changes poll", &l, &r);
}

#[test]
fn parity_changes_baseline() {
    let p = parity();
    let out = p._temp.path().join("baseline.jsonl");
    let out_arg = out.to_string_lossy().into_owned();
    let (l, r) = p.run(&["changes", "baseline", "--out", &out_arg, "--json"]);
    assert_parity("changes baseline", &l, &r);
    // run_both executes local then served, so the surviving file is the served
    // arm's snapshot; it must carry the shared fixture entities.
    let snapshot = std::fs::read_to_string(&out).expect("baseline snapshot file");
    assert!(
        snapshot.lines().any(|line| line.contains("Alice")),
        "the snapshot carries the fixture entities: {snapshot}"
    );
    assert!(
        !snapshot.contains("\"baseline\""),
        "the terminal handshake is not written into the snapshot file"
    );
}
