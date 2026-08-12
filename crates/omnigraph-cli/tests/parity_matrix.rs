//! RFC-009 Phase 1 — the embedded/remote parity referee.
//!
//! For every CLI verb with an `is_remote` fork, run the identical
//! invocation against (a) the local graph directly and (b) a spawned
//! server on a twin copy of the same graph, with the SAME actor on both
//! arms (local `--as act-parity`; remote bearer token resolving to
//! `act-parity`). Scrub the declared-volatile allowlist
//! (`support::scrub_volatile` — ids, wall-clock, transport locations);
//! everything else must match exactly.
//!
//! This test PINS behavior; it does not idealize it. Genuine divergences
//! discovered here are recorded in `KNOWN_DIVERGENCES` below (and filed),
//! never silently repaired — repairs are Phase 3's job, gated by this
//! referee staying green through the refactor.

use tempfile::TempDir;

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
            commit["manifest_version"].as_u64().is_some(),
            "{verb}: {arm} receipt must carry manifest_version"
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
    let (l, r) = p.run(&["branch", "merge", "feature", "--into", "main", "--json"]);
    assert_parity("branch merge", &l, &r);
    // `--delete-branch` composes merge + delete at each arm's own boundary
    // (embedded: two engine calls; remote: the server handler) — this row is
    // the referee that keeps the two composition sites from drifting.
    let (l, r) = p.run(&["branch", "create", "--from", "main", "feature2", "--json"]);
    assert_parity("branch create (delete-branch setup)", &l, &r);
    let (l, r) = p.run(&[
        "branch",
        "merge",
        "feature2",
        "--into",
        "main",
        "--delete-branch",
        "--json",
    ]);
    assert_parity("branch merge --delete-branch", &l, &r);
    let (l, r) = p.run(&["branch", "list", "--json"]);
    assert_parity("branch list (post delete-branch)", &l, &r);
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
}

#[test]
fn parity_export() {
    let p = parity();
    let (l, r) = p.run(&["export"]);
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

    // Discovery (parity HOLDS, behavior surprising): an inline query run
    // with a declared-but-unbound param does NOT error on either arm — it
    // returns every row (the filter drops), while the stored-query invoke
    // path hard-errors 'parameter not provided'. Pinned here as agreeing
    // behavior; the cross-path asymmetry is filed separately.
    let (l, r) = p.run(&[
        "query",
        "--query",
        query.to_str().unwrap(),
        "get_person",
        "--json",
    ]);
    assert_eq!(
        (l.status.success(), r.status.success()),
        (true, true),
        "unbound-param inline query currently SUCCEEDS on both arms (matches-all)"
    );
}

// ---- documented exclusions (not bugs; the Phase 4 capability table) ----
//
// - `graphs list`: server-only today; becomes Both-capability when the
//   embedded arm enumerates the cluster catalog (RFC-009 open Q3, answered).
// - `ingest`: deprecated compatibility loader; its remote arm rides the
//   deprecated JSON /ingest route. Canonical `load` is strict graph-batch on
//   both arms; the remote arm sends raw NDJSON to `/load/ndjson`.
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
