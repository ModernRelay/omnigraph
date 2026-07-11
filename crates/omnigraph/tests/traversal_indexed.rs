//! BTREE-indexed Expand path (`execute_expand_indexed`) coverage.
//!
//! These tests force the Expand execution mode via the scoped `with_traversal_mode`
//! test seam — NOT the process-global `OMNIGRAPH_TRAVERSAL_MODE` env var — and
//! assert the indexed path matches the CSR path (both are semantically identical:
//! the indexed path serves neighbor lookups from the persisted src/dst BTREE
//! instead of an in-memory CSR). The seam is scope-bound and process-safe, so
//! these tests need no `#[serial]` and no dedicated binary.

mod helpers;

use omnigraph::db::Omnigraph;
use omnigraph::instrumentation::with_traversal_mode;
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph::table_store::{IndexCoverage, TableStore};
use omnigraph_compiler::ir::ParamMap;

use helpers::*;

/// Run `name` on main under the cost-chooser (auto) Expand mode; first column sorted.
async fn sorted_names(db: &mut Omnigraph, queries: &str, name: &str, params: &ParamMap) -> Vec<String> {
    first_column_sorted(&query_main(db, queries, name, params).await.unwrap())
}

/// Run the same query under CSR, indexed, and auto (cost-chooser) modes; assert
/// all three produce identical results and return them. The forced modes use the
/// scoped `with_traversal_mode` seam; the auto pass exercises `choose_expand_mode`
/// end to end (whichever path it selects, the rows must match the forced paths —
/// the chooser changes which path runs, never the result).
async fn both_modes(db: &mut Omnigraph, queries: &str, name: &str, params: &ParamMap) -> Vec<String> {
    let csr = first_column_sorted(
        &with_traversal_mode("csr", query_main(db, queries, name, params))
            .await
            .unwrap(),
    );
    let indexed = first_column_sorted(
        &with_traversal_mode("indexed", query_main(db, queries, name, params))
            .await
            .unwrap(),
    );
    let auto = sorted_names(db, queries, name, params).await;
    assert_eq!(
        indexed, csr,
        "indexed Expand must produce identical results to CSR for query '{name}'"
    );
    assert_eq!(
        auto, csr,
        "auto (cost-chooser) Expand must produce identical results to the forced paths for query '{name}'"
    );
    indexed
}

// The C6 index-coverage guard: `key_column_index_coverage` must report whether
// a `key_col IN (...)` scan will use the persisted BTREE or silently full-scan.
#[tokio::test]
async fn key_column_index_coverage_detects_btree_presence() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let snap = snapshot_main(&db).await.unwrap();

    // The shared fixture explicitly reconciles indexes after loading, so the
    // edge `src` BTREE is present and fully covered here.
    let edge_ds = snap.open("edge:Knows").await.unwrap();
    let src_cov = TableStore::key_column_index_coverage(&edge_ds, "src")
        .await
        .unwrap();
    assert_eq!(src_cov, IndexCoverage::Indexed, "edge src is BTREE-indexed");

    // A node property column with no scalar index → Degraded (the warn path).
    let node_ds = snap.open("node:Person").await.unwrap();
    let age_cov = TableStore::key_column_index_coverage(&node_ds, "age")
        .await
        .unwrap();
    assert!(
        matches!(age_cov, IndexCoverage::Degraded { .. }),
        "non-indexed column should be Degraded, got {age_cov:?}"
    );
}

// An edge appended after the BTREE was built lands in a new fragment that the
// index does not cover (edge-index creation is skipped once a BTREE exists). The
// scan is then partly a full scan, so coverage must report `Degraded` — otherwise
// the cost chooser would price an unindexed-in-part scan as fully indexed.
// (Results stay correct regardless — `indexed_finds_unindexed_appended_edge`.)
#[tokio::test]
async fn coverage_degrades_for_appended_unindexed_fragment() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // The fixture's explicit post-load `ensure_indices` covers every current
    // Knows fragment → Indexed.
    let snap = snapshot_main(&db).await.unwrap();
    let edge_ds = snap.open("edge:Knows").await.unwrap();
    assert_eq!(
        TableStore::key_column_index_coverage(&edge_ds, "src").await.unwrap(),
        IndexCoverage::Indexed,
        "freshly-loaded edge BTREE covers all fragments"
    );

    // Append an edge → a new, unindexed fragment outside the index fragment_bitmap.
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "add_friend",
        &params(&[("$from", "Alice"), ("$to", "Diana")]),
    )
    .await
    .unwrap();

    let snap2 = snapshot_main(&db).await.unwrap();
    let edge_ds2 = snap2.open("edge:Knows").await.unwrap();
    let cov = TableStore::key_column_index_coverage(&edge_ds2, "src").await.unwrap();
    assert!(
        matches!(cov, IndexCoverage::Degraded { .. }),
        "appended unindexed fragment must degrade coverage, got {cov:?}"
    );
}

#[tokio::test]
async fn indexed_matches_csr_one_hop_same_type() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    // friends_of: `$p knows $f` (Person -> Person, single hop).
    let got = both_modes(&mut db, TEST_QUERIES, "friends_of", &params(&[("$name", "Alice")])).await;
    assert_eq!(got, vec!["Bob", "Charlie"], "Alice knows Bob and Charlie");
}

#[tokio::test]
async fn indexed_matches_csr_undirected_one_hop() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let queries = r#"
query connected($name: String) {
    match {
        $p: Person { name: $name }
        $p <knows> $f
    }
    return { $f.name }
}
"#;
    // Bob: outgoing Bob->Diana, incoming Alice->Bob — undirected sees both.
    let got = both_modes(&mut db, queries, "connected", &params(&[("$name", "Bob")])).await;
    assert_eq!(got, vec!["Alice", "Diana"], "out ∪ in neighbors of Bob");
}

/// The undirected cost fix (review follow-up): coverage is priced by the
/// WORST of the two probed columns. This test degrades coverage via a
/// mutation-appended unindexed fragment and asserts undirected results stay
/// correct and mode-equivalent regardless of which path the cost model picks.
#[tokio::test]
async fn indexed_matches_csr_undirected_under_degraded_coverage() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // Append an edge through the mutation path — an unindexed fragment that
    // degrades BTREE coverage on the Knows table (pinned by the coverage test
    // above). Diana->Alice also gives Alice a NEW incoming edge that only the
    // undirected form can see from Alice's side.
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "add_friend",
        &params(&[("$from", "Diana"), ("$to", "Alice")]),
    )
    .await
    .unwrap();

    let queries = r#"
query connected($name: String) {
    match {
        $p: Person { name: $name }
        $p <knows> $f
    }
    return { $f.name }
}
"#;
    // Alice: outgoing Bob, Charlie; incoming Diana (the fresh unindexed edge).
    let got = both_modes(&mut db, queries, "connected", &params(&[("$name", "Alice")])).await;
    assert_eq!(got, vec!["Bob", "Charlie", "Diana"]);
}

#[tokio::test]
async fn indexed_matches_csr_multi_hop_same_type() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let queries = r#"
query reach($name: String) {
    match {
        $p: Person { name: $name }
        $p knows{1,2} $f
    }
    return { $f.name }
}
"#;
    // Alice -> Bob, Charlie (1 hop); Bob -> Diana (2 hops).
    let got = both_modes(&mut db, queries, "reach", &params(&[("$name", "Alice")])).await;
    assert_eq!(got, vec!["Bob", "Charlie", "Diana"]);
}

#[tokio::test]
async fn indexed_matches_csr_cross_type() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let queries = r#"
query employer($name: String) {
    match {
        $p: Person { name: $name }
        $p worksAt $c
    }
    return { $c.name }
}
"#;
    let got = both_modes(&mut db, queries, "employer", &params(&[("$name", "Alice")])).await;
    assert_eq!(got, vec!["Acme"], "Alice works at Acme");
}

#[tokio::test]
async fn indexed_matches_csr_no_match() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    // Diana has no outgoing Knows edges → empty in both modes.
    let got = both_modes(&mut db, TEST_QUERIES, "friends_of", &params(&[("$name", "Diana")])).await;
    assert!(got.is_empty(), "Diana knows no one");
}

#[tokio::test]
async fn indexed_finds_unindexed_appended_edge() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // Append Alice -> Diana AFTER the initial load. `ensure_indices`' existence
    // guard means the src/dst BTREE built on the first load does NOT cover this
    // new fragment. The indexed path must still find it via Lance's
    // unindexed-fragment scan (fast_search=false default), so partial index
    // coverage never silently drops rows.
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "add_friend",
        &params(&[("$from", "Alice"), ("$to", "Diana")]),
    )
    .await
    .unwrap();

    let got = first_column_sorted(
        &with_traversal_mode(
            "indexed",
            query_main(&mut db, TEST_QUERIES, "friends_of", &params(&[("$name", "Alice")])),
        )
        .await
        .unwrap(),
    );

    assert_eq!(
        got,
        vec!["Bob", "Charlie", "Diana"],
        "indexed traversal must see the freshly-appended, unindexed edge"
    );
}

// Regression: a node `id` is unique only WITHIN a type, so a `Person` and a
// `Company` can share an id string. A variable-length traversal over a
// cross-type edge (`worksAt`, Person -> Company) must structurally stop after
// one hop — a Company is not a `worksAt` source — so `worksAt{1,2}` returns
// exactly the one-hop companies. Before the structural hop-cap, the indexed
// path's single string interner de-interned the hop-1 Company id back to the
// colliding Person id and ran a hop-2 `worksAt src IN (...)` scan that matched
// that same-string Person's edges, emitting a spurious second-hop company the
// CSR path never produces. `both_modes` (csr == indexed == auto) plus the
// golden assert catch both the divergence and an over-emitting shared bug.
#[tokio::test]
async fn cross_type_id_collision_does_not_bleed_into_second_hop() {
    const SCHEMA: &str = r#"
node Person { name: String @key }
node Company { name: String @key }
edge WorksAt: Person -> Company
"#;
    // `shared` is BOTH a Person id and a Company id. alice worksAt the Company
    // `shared`; the Person `shared` worksAt the Company `other`.
    const DATA: &str = r#"{"type":"Person","data":{"name":"alice"}}
{"type":"Person","data":{"name":"shared"}}
{"type":"Company","data":{"name":"shared"}}
{"type":"Company","data":{"name":"other"}}
{"edge":"WorksAt","from":"alice","to":"shared"}
{"edge":"WorksAt","from":"shared","to":"other"}"#;
    const QUERY: &str = r#"
query reach($name: String) {
    match {
        $p: Person { name: $name }
        $p worksAt{1,2} $c
    }
    return { $c.name }
}
"#;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, SCHEMA).await.unwrap();
    load_jsonl(&mut db, DATA, LoadMode::Overwrite).await.unwrap();

    let got = both_modes(&mut db, QUERY, "reach", &params(&[("$name", "alice")])).await;
    assert_eq!(
        got,
        vec!["shared"],
        "cross-type worksAt{{1,2}} must return only the one-hop company; a hop-2 \
         result means the id-string collision bled across types"
    );
}

const REACH_5: &str = r#"
query reach($name: String) {
    match {
        $p: Person { name: $name }
        $p knows{1,5} $f
    }
    return { $f.name }
}
"#;

// A directed 3-cycle a->b->c->a, traversed with a hop ceiling (5) ABOVE the cycle
// length. Variable-length traversal must terminate and dedup (the source is
// seeded into `visited`, so the c->a back-edge does not re-emit a). Uses a
// bounded range deliberately: an unbounded `{1,}` is a typecheck error, not a
// runtime path. `both_modes` also confirms indexed == csr on the cycle.
#[tokio::test]
async fn variable_hops_terminate_and_dedup_on_cycle() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let data = r#"{"type":"Person","data":{"name":"a"}}
{"type":"Person","data":{"name":"b"}}
{"type":"Person","data":{"name":"c"}}
{"edge":"Knows","from":"a","to":"b"}
{"edge":"Knows","from":"b","to":"c"}
{"edge":"Knows","from":"c","to":"a"}"#;
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&mut db, data, LoadMode::Overwrite).await.unwrap();

    let got = both_modes(&mut db, REACH_5, "reach", &params(&[("$name", "a")])).await;
    // From a: b (1 hop), c (2 hops); the c->a back-edge hits the seeded source
    // and is not re-emitted. No infinite loop, each node at most once.
    assert_eq!(got, vec!["b", "c"]);
}

// A self-loop a->a plus a->b. Variable-length traversal must not loop forever and
// must not re-emit the seeded source.
#[tokio::test]
async fn variable_hops_handle_self_loop() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let data = r#"{"type":"Person","data":{"name":"a"}}
{"type":"Person","data":{"name":"b"}}
{"edge":"Knows","from":"a","to":"a"}
{"edge":"Knows","from":"a","to":"b"}"#;
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&mut db, data, LoadMode::Overwrite).await.unwrap();

    let got = both_modes(&mut db, REACH_5, "reach", &params(&[("$name", "a")])).await;
    // a->a hits the seeded source (pruned); only b is reached.
    assert_eq!(got, vec!["b"]);
}
