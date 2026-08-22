//! The traversal refactor's three behavioral additions (issue #533 arc):
//!
//! 1. **Mid-traversal mode switch** — a variable-length expand that starts on
//!    the indexed path re-decides per hop with the observed frontier and swaps
//!    to CSR when it has outgrown the dispatch decision, carrying its BFS state
//!    across the swap. Results are contracted identical to both forced modes.
//! 2. **Limit pushdown** — an unordered trailing `limit` on a final filterless
//!    Expand bounds the traversal's emitted pairs; results are a valid subset
//!    of the uncapped answer, exactly `limit` rows when enough exist.
//! 3. **Persisted adjacency artifact** — `optimize` writes it, traversals load
//!    it, staleness and corruption are rejected fail-open.
//!
//! Mode forcing uses the scoped `with_traversal_mode` seam, never the env var.

mod helpers;

use std::collections::HashSet;

use omnigraph::db::Omnigraph;
use omnigraph::instrumentation::with_traversal_mode;
use omnigraph::loader::{LoadMode, load_jsonl};

use helpers::*;

/// A 50-node directed clique (every ordered pair, no self-loops): 2,450 edges,
/// fanout 49. A `{1,3}` traversal from one seed observes a hop-2 frontier of
/// 49 with observed growth 49x, whose projected remaining work exceeds the
/// CSR build cost — the mid-traversal switch fires while the initial dispatch
/// (frontier 1, estimate 3·1·49 = 147 vs build 3,675) chose indexed. The same
/// shape in miniature as the issue's IMDb reproduction.
fn clique_jsonl(n: usize) -> String {
    let mut s = String::new();
    for i in 0..n {
        s.push_str(&format!(
            r#"{{"type":"Person","data":{{"name":"p{}"}}}}"#,
            i
        ));
        s.push('\n');
    }
    for i in 0..n {
        for j in 0..n {
            if i != j {
                s.push_str(&format!(
                    r#"{{"edge":"Knows","from":"p{}","to":"p{}"}}"#,
                    i, j
                ));
                s.push('\n');
            }
        }
    }
    s
}

const REACH_3: &str = r#"
query reach($name: String) {
    match {
        $p: Person { name: $name }
        $p knows{1,3} $f
    }
    return { $f.name }
}
"#;

const REACH_3_CAPPED: &str = r#"
query reach_capped($name: String) {
    match {
        $p: Person { name: $name }
        $p knows{1,3} $f
    }
    return { $f.name }
    limit 10
}
"#;

async fn clique_db(dir: &tempfile::TempDir) -> Omnigraph {
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&db, &clique_jsonl(50), LoadMode::Overwrite)
        .await
        .unwrap();
    // The endpoint BTREEs must exist, or dispatch prices a degraded scan and
    // takes CSR outright — the switch under test would never arm.
    db.optimize().await.unwrap();
    db
}

// The switch-arming shape: auto must agree exactly with both forced modes on
// a traversal whose frontier outgrows its dispatch decision mid-flight. This
// is the acceptance shape for #533 — before the per-hop re-decision, auto
// followed indexed here; the row contract is what pins the switch as safe.
#[tokio::test]
async fn adaptive_switch_returns_identical_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = clique_db(&dir).await;
    let p = params(&[("$name", "p0")]);

    let csr = first_column_sorted(
        &with_traversal_mode("csr", query_main(&mut db, REACH_3, "reach", &p))
            .await
            .unwrap(),
    );
    let indexed = first_column_sorted(
        &with_traversal_mode("indexed", query_main(&mut db, REACH_3, "reach", &p))
            .await
            .unwrap(),
    );
    let auto = first_column_sorted(&query_main(&mut db, REACH_3, "reach", &p).await.unwrap());

    assert_eq!(csr.len(), 49, "p0 reaches every other clique member");
    assert_eq!(indexed, csr, "forced modes agree");
    assert_eq!(
        auto, csr,
        "auto (dispatch + mid-traversal re-decision) must match the forced paths"
    );
}

// The switch under an UNDIRECTED traversal (Direction::Both — two endpoint
// probes per hop, the probe-factor case, CSR+CSC merge after the swap). Same
// clique, so the hop-2 policy check sees the same explosive growth.
#[tokio::test]
async fn adaptive_switch_undirected_returns_identical_rows() {
    const REACH_3_UNDIRECTED: &str = r#"
query reach_u($name: String) {
    match {
        $p: Person { name: $name }
        $p <knows>{1,3} $f
    }
    return { $f.name }
}
"#;
    let dir = tempfile::tempdir().unwrap();
    let mut db = clique_db(&dir).await;
    let p = params(&[("$name", "p0")]);
    let csr = first_column_sorted(
        &with_traversal_mode(
            "csr",
            query_main(&mut db, REACH_3_UNDIRECTED, "reach_u", &p),
        )
        .await
        .unwrap(),
    );
    let auto = first_column_sorted(
        &query_main(&mut db, REACH_3_UNDIRECTED, "reach_u", &p)
            .await
            .unwrap(),
    );
    assert_eq!(csr.len(), 49);
    assert_eq!(auto, csr, "undirected auto must match forced CSR");
}

// min_hops >= 2 across the swap: the swap fires entering hop 2 (before any
// emission has happened), so `seen_dst` and the min-hop emission gate must
// survive the id-space translation. The clique makes every member 1 hop away
// (excluded by min 2); a chain hanging off p1 provides the only >= 2-hop
// destinations.
#[tokio::test]
async fn min_hops_gate_survives_the_switch() {
    const REACH_2_3: &str = r#"
query far($name: String) {
    match {
        $p: Person { name: $name }
        $p knows{2,3} $f
    }
    return { $f.name }
}
"#;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut jsonl = clique_jsonl(50);
    jsonl.push_str(r#"{"type":"Person","data":{"name":"q1"}}"#);
    jsonl.push('\n');
    jsonl.push_str(r#"{"type":"Person","data":{"name":"q2"}}"#);
    jsonl.push('\n');
    jsonl.push_str(r#"{"edge":"Knows","from":"p1","to":"q1"}"#);
    jsonl.push('\n');
    jsonl.push_str(r#"{"edge":"Knows","from":"q1","to":"q2"}"#);
    jsonl.push('\n');
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&db, &jsonl, LoadMode::Overwrite).await.unwrap();
    db.optimize().await.unwrap();

    let p = params(&[("$name", "p0")]);
    let csr = first_column_sorted(
        &with_traversal_mode("csr", query_main(&mut db, REACH_2_3, "far", &p))
            .await
            .unwrap(),
    );
    let indexed = first_column_sorted(
        &with_traversal_mode("indexed", query_main(&mut db, REACH_2_3, "far", &p))
            .await
            .unwrap(),
    );
    let auto = first_column_sorted(&query_main(&mut db, REACH_2_3, "far", &p).await.unwrap());
    // Every clique member is 1 hop from p0 (excluded); q1 is 2 hops (via p1),
    // q2 is 3 hops.
    assert_eq!(csr, vec!["q1", "q2"]);
    assert_eq!(indexed, csr);
    assert_eq!(auto, csr, "min-hop emission gate must survive the swap");
}

// Limit pushdown returns exactly `limit` rows, each a member of the uncapped
// answer, in every mode. The specific subset is not pinned — an unordered
// limit is contracted as ANY n valid rows.
#[tokio::test]
async fn capped_unordered_limit_returns_valid_subset() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = clique_db(&dir).await;
    let p = params(&[("$name", "p0")]);

    let full: HashSet<String> =
        first_column_sorted(&query_main(&mut db, REACH_3, "reach", &p).await.unwrap())
            .into_iter()
            .collect();
    assert_eq!(full.len(), 49);

    for mode in [None, Some("csr"), Some("indexed")] {
        let fut = query_main(&mut db, REACH_3_CAPPED, "reach_capped", &p);
        let result = match mode {
            Some(m) => with_traversal_mode(m, fut).await.unwrap(),
            None => fut.await.unwrap(),
        };
        let rows = first_column_sorted(&result);
        assert_eq!(
            rows.len(),
            10,
            "capped query returns exactly the limit (mode {mode:?})"
        );
        for row in &rows {
            assert!(
                full.contains(row),
                "capped row '{row}' must belong to the uncapped answer (mode {mode:?})"
            );
        }
    }
}

// A limit larger than the answer must not truncate it.
#[tokio::test]
async fn cap_above_answer_size_returns_everything() {
    const REACH_BIG_LIMIT: &str = r#"
query reach_big($name: String) {
    match {
        $p: Person { name: $name }
        $p knows{1,3} $f
    }
    return { $f.name }
    limit 100000
}
"#;
    let dir = tempfile::tempdir().unwrap();
    let mut db = clique_db(&dir).await;
    let p = params(&[("$name", "p0")]);
    let rows = first_column_sorted(
        &query_main(&mut db, REACH_BIG_LIMIT, "reach_big", &p)
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 49, "a generous limit changes nothing");
}

// `optimize` persists the adjacency artifact at its fixed store path, in the
// binary format (magic + version + header + verbatim arrays).
#[tokio::test]
async fn optimize_persists_the_graph_index_artifact() {
    let dir = tempfile::tempdir().unwrap();
    let _db = clique_db(&dir).await;
    let artifact = dir.path().join("__graph_index/csr-current.bin");
    assert!(
        artifact.is_file(),
        "optimize must write the persisted adjacency artifact"
    );
    let body = std::fs::read(&artifact).unwrap();
    assert_eq!(&body[..8], b"OGCSRIDX", "binary artifact magic");
    assert_eq!(
        u32::from_le_bytes(body[8..12].try_into().unwrap()),
        1,
        "format version 1"
    );
    // The header is JSON and carries the digest + identity stamps.
    let header_len = u64::from_le_bytes(body[12..20].try_into().unwrap()) as usize;
    let header = std::str::from_utf8(&body[20..20 + header_len]).unwrap();
    assert!(header.contains("\"payload_sha256_b64\""));
    assert!(header.contains("\"tables\""));
}

// Fail-open: a corrupt artifact must be rejected and rebuilt around, never
// change results or error the query.
#[tokio::test]
async fn corrupt_artifact_falls_back_to_the_scan_build() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = clique_db(&dir).await;
    let p = params(&[("$name", "p0")]);
    let before = first_column_sorted(
        &with_traversal_mode("csr", query_main(&mut db, REACH_3, "reach", &p))
            .await
            .unwrap(),
    );

    let artifact = dir.path().join("__graph_index/csr-current.bin");
    // Truncate mid-body: the section walk overruns, the loader logs and
    // rebuilds in memory.
    let body = std::fs::read(&artifact).unwrap();
    std::fs::write(&artifact, &body[..body.len() / 2]).unwrap();

    // Fresh handle so no in-memory cached index masks the artifact read.
    let mut db2 = Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap();
    let after = first_column_sorted(
        &with_traversal_mode("csr", query_main(&mut db2, REACH_3, "reach", &p))
            .await
            .unwrap(),
    );
    assert_eq!(after, before, "corrupt artifact must not change results");
}

// Staleness: an edge written AFTER the artifact was persisted changes that
// table's identity stamp; a traversal touching that edge must reject the
// artifact and see the new edge.
#[tokio::test]
async fn stale_artifact_is_rejected_after_an_edge_write() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    db.optimize().await.unwrap();
    let artifact = dir.path().join("__graph_index/csr-current.bin");
    assert!(artifact.is_file());

    // Alice knows Bob, Charlie. Append Alice -> Diana AFTER the artifact.
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "add_friend",
        &params(&[("$from", "Alice"), ("$to", "Diana")]),
    )
    .await
    .unwrap();

    // Fresh handle: the artifact on the store is now stale for edge:Knows.
    let mut db2 = Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap();
    let got = first_column_sorted(
        &with_traversal_mode(
            "csr",
            query_main(
                &mut db2,
                TEST_QUERIES,
                "friends_of",
                &params(&[("$name", "Alice")]),
            ),
        )
        .await
        .unwrap(),
    );
    assert_eq!(
        got,
        vec!["Bob", "Charlie", "Diana"],
        "a stale artifact must lose to the freshly-built index"
    );
}
