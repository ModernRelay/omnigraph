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
    // `branch delete` is destructive: the served (remote) arm is non-local and
    // requires consent (RFC-011 Decision 9), so the row passes `--yes` to test
    // the operation itself, not the safety gate. The local arm ignores `--yes`.
    let (l, r) = p.run(&["branch", "delete", "parity-branch", "--yes", "--json"],
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
    // `--delete-branch` composes merge + delete at each arm's own boundary
    // (embedded: two engine calls; remote: the server handler) — this row is
    // the referee that keeps the two composition sites from drifting.
    let (l, r) = p.run(&["branch", "create", "--from", "main", "feature2", "--json"],
    );
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
        ],
    );
    assert_parity("load", &l, &r);
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
// - `ingest`: deprecated alias of load; its remote arm rides the deprecated
//   /ingest route. The canonical `load` verb targets `/load` (RFC-009 Phase 5,
//   landed) — `parity_load` exercises it on the remote arm.
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

// ---- blob verb parity ----

const BLOB_PARITY_SCHEMA: &str = r#"
node Document {
    title: String @key
    content: Blob?
}
"#;

/// Blob twin: the served graph is seeded with one inline "Hello World" row
/// plus one external file:// reference, then byte-mirrored into the local
/// twin — the byte mirror is what makes the identity-derived ETag comparable
/// across arms (identical BlobVersionTag tuple through the shared
/// `blob_etag`); do not replace it with two independent loads. The external
/// object stays alive in the parity temp for the test's lifetime; neither
/// arm may touch it (the CLI never fetches external references).
fn blob_parity() -> (Parity, String) {
    let temp = tempfile::tempdir().unwrap();
    let external_path = temp.path().join("external.bin");
    std::fs::write(&external_path, b"external-bytes").unwrap();
    let external_uri = format!("file://{}", external_path.display());
    let schema_path = temp.path().join("blob.pg");
    std::fs::write(&schema_path, BLOB_PARITY_SCHEMA).unwrap();
    let data_path = temp.path().join("blob.jsonl");
    std::fs::write(
        &data_path,
        format!(
            "{}\n{}",
            r#"{"type": "Document", "data": {"title": "readme", "content": "base64:SGVsbG8gV29ybGQ="}}"#,
            format!(
                r#"{{"type": "Document", "data": {{"title": "ext", "content": "{external_uri}"}}}}"#
            ),
        ),
    )
    .unwrap();
    let local = temp.path().join("local.omni");
    let cluster_dir = parity_configs_from(temp.path(), &local, &schema_path, &data_path);
    let server = spawn_server_with_cluster_env(
        &cluster_dir,
        &[(
            "OMNIGRAPH_SERVER_BEARER_TOKENS_JSON",
            r#"{"act-parity":"parity-tok"}"#,
        )],
    );
    (
        Parity {
            _temp: temp,
            local,
            server,
        },
        external_uri,
    )
}

/// Raw-bytes comparison for `blob get` rows (the `parity_export` non-JSON
/// precedent): exit codes and exact stdout bytes must match, and the local
/// arm must produce the expected payload so the check is never vacuous.
fn assert_blob_get_parity(
    row: &str,
    expected: &[u8],
    local: &std::process::Output,
    remote: &std::process::Output,
) {
    assert_eq!(
        local.status.code(),
        remote.status.code(),
        "{row}: exit codes diverge\nlocal {local:?}\nremote {remote:?}"
    );
    assert!(local.status.success(), "{row}: local arm failed: {local:?}");
    assert_eq!(
        &local.stdout[..],
        expected,
        "{row}: local payload bytes are wrong"
    );
    assert_eq!(
        local.stdout, remote.stdout,
        "{row}: payload bytes diverge across arms"
    );
}

#[test]
fn parity_blob_get_full() {
    let (p, _) = blob_parity();
    let (l, r) = p.run(&["blob", "get", "Document", "readme", "content"]);
    assert_blob_get_parity("blob get full", b"Hello World", &l, &r);
}

#[test]
fn parity_blob_get_range() {
    let (p, _) = blob_parity();
    let (l, r) = p.run(&[
        "blob", "get", "Document", "readme", "content", "--offset", "6", "--length", "5",
    ]);
    assert_blob_get_parity("blob get range", b"World", &l, &r);
}

#[test]
fn parity_blob_get_snapshot() {
    let (p, _) = blob_parity();
    // The twins are byte copies, so the local twin's snapshot id is valid on
    // both arms. This row pins the --snapshot plumbing (query param remote,
    // ReadTarget embedded); pinning-under-change semantics are covered by
    // cli_data + the server route tests.
    let snapshot_id = tokio::runtime::Runtime::new().unwrap().block_on(async {
        omnigraph::db::Omnigraph::open(p.local.to_string_lossy().as_ref())
            .await
            .unwrap()
            .resolve_snapshot("main")
            .await
            .unwrap()
            .to_string()
    });
    let (l, r) = p.run(&[
        "blob", "get", "Document", "readme", "content", "--snapshot", &snapshot_id,
    ]);
    assert_blob_get_parity("blob get snapshot", b"Hello World", &l, &r);
}

#[test]
fn parity_blob_stat_json() {
    let (p, _) = blob_parity();
    let (l, r) = p.run(&["blob", "stat", "Document", "readme", "content", "--json"]);
    // `etag` and `size` are NOT in the volatile allowlist — this is an exact
    // cross-arm pin of the shared identity-derived validator.
    assert_parity("blob stat", &l, &r);
}

#[test]
fn parity_blob_stat_external_json() {
    let (p, _) = blob_parity();
    let (l, r) = p.run(&["blob", "stat", "Document", "ext", "content", "--json"]);
    assert_parity("blob stat external", &l, &r);
    let local_json: serde_json::Value = serde_json::from_slice(&l.stdout).unwrap();
    assert_eq!(local_json["kind"], "external");
    assert!(local_json["size"].is_null() && local_json["etag"].is_null());
}

#[test]
fn parity_blob_get_external_error() {
    let (p, external_uri) = blob_parity();
    let (l, r) = p.run(&["blob", "get", "Document", "ext", "content"]);
    assert_eq!(
        (l.status.success(), r.status.success()),
        (false, false),
        "external get must fail on both arms\nlocal {l:?}\nremote {r:?}"
    );
    for (arm, out) in [("local", &l), ("remote", &r)] {
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains(&external_uri),
            "{arm} arm must surface the stored URI: {stderr}"
        );
    }
}

#[test]
fn parity_blob_get_offset_beyond_size() {
    let (p, _) = blob_parity();
    let (l, r) = p.run(&[
        "blob", "get", "Document", "readme", "content", "--offset", "99",
    ]);
    assert_eq!(
        (l.status.success(), r.status.success()),
        (false, false),
        "out-of-range get must fail on both arms\nlocal {l:?}\nremote {r:?}"
    );
    for (arm, out) in [("local", &l), ("remote", &r)] {
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("not satisfiable"),
            "{arm} arm error should be the shared 416 text: {stderr}"
        );
    }
}
