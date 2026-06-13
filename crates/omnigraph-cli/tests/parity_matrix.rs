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

/// One matched setup per row: twin graphs + the SAME Cedar bundle on both
/// arms (the local arm via --config top-level policy.file; the server via
/// its config). Returns everything a row needs.
struct Parity {
    _temp: TempDir,
    local: std::path::PathBuf,
    local_cfg: std::path::PathBuf,
    server: TestServer,
}

fn parity() -> Parity {
    let (temp, local, remote) = twin_graphs();
    let (local_cfg, server_cfg) = parity_configs(temp.path(), &local, &remote);
    let server = spawn_server_with_config_env(
        &server_cfg,
        &[(
            "OMNIGRAPH_SERVER_BEARER_TOKENS_JSON",
            r#"{"act-parity":"parity-tok"}"#,
        )],
    );
    Parity {
        _temp: temp,
        local,
        local_cfg,
        server,
    }
}

impl Parity {
    fn run(&self, args: &[&str]) -> (std::process::Output, std::process::Output) {
        run_both_with_config(&self.local, Some(&self.local_cfg), &self.server.base_url, args)
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

#[test]
fn parity_query() {
    let p = parity();
    let query = fixture("test.gq");
    let (l, r) = p.run(&[
            "query",
            "--query",
            query.to_str().unwrap(),
            "--name",
            "get_person",
            "--params",
            r#"{"name":"Alice"}"#,
            "--json",
        ],
    );
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
fn parity_mutate() {
    let p = parity();
    let (l, r) = p.run(&[
            "mutate",
            "-e",
            "query add($name: String, $age: I32) { insert Person { name: $name, age: $age } }",
            "--params",
            r#"{"name":"Parity","age":7}"#,
            "--json",
        ],
    );
    assert_parity("mutate", &l, &r);
}

#[test]
fn parity_branch_create_delete() {
    let p = parity();
    let (l, r) = p.run(&["branch", "create", "--from", "main", "parity-branch", "--json"],
    );
    assert_parity("branch create", &l, &r);
    let (l, r) = p.run(&["branch", "delete", "parity-branch", "--json"],
    );
    assert_parity("branch delete", &l, &r);
}

#[test]
fn parity_branch_merge() {
    let p = parity();
    let (l, r) = p.run(&["branch", "create", "--from", "main", "feature", "--json"],
    );
    assert_parity("branch create (merge setup)", &l, &r);
    let (l, r) = p.run(&["branch", "merge", "feature", "--into", "main", "--json"],
    );
    assert_parity("branch merge", &l, &r);
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
        ],
    );
    assert_parity("load", &l, &r);
}

// ---- error parity: exit codes must match for shared failure cases ----

#[test]
fn parity_errors_share_exit_codes() {
    let p = parity();

    // unknown branch on merge
    let (l, r) = p.run(&["branch", "merge", "no-such-branch", "--into", "main", "--json"],
    );
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
            "--name",
            "no_such_query",
            "--json",
        ],
    );
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
            "--name",
            "get_person",
            "--json",
        ],
    );
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
// - `ingest`: deprecated alias of load; the remote `load` arm itself rides
//   the deprecated /ingest route today (RFC-009 Phase 5 flips it to /load —
//   this matrix's `parity_load` row is where that flip becomes visible).
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
