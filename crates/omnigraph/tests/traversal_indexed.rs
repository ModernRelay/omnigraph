//! BTREE-indexed Expand path (`execute_expand_indexed`) coverage.
//!
//! These tests force the Expand execution mode via `OMNIGRAPH_TRAVERSAL_MODE`
//! and assert the indexed path matches the CSR path (both are semantically
//! identical — the indexed path just serves neighbor lookups from the persisted
//! src/dst BTREE instead of an in-memory CSR). They live in their own test
//! binary and are all `#[serial]`, so the env writes never race a concurrent
//! reader: within this process serial execution serializes every env read, and
//! other test binaries (e.g. `traversal.rs`) are separate processes whose env
//! stays unset (→ CSR), validating the shared hydrate/align tail on the CSR path.

mod helpers;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::{Array, StringArray};

use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::instrumentation::{QueryIoProbes, with_query_io_probes};
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph::table_store::{IndexCoverage, TableStore};
use omnigraph_compiler::ir::ParamMap;
use omnigraph_compiler::result::QueryResult;
use serial_test::serial;

use helpers::*;

fn set_mode(mode: &str) {
    // SAFE: every test here is #[serial] and this binary has no non-serial
    // env reader, so no thread reads the environment during this write.
    unsafe { std::env::set_var("OMNIGRAPH_TRAVERSAL_MODE", mode) };
}

fn clear_mode() {
    unsafe { std::env::remove_var("OMNIGRAPH_TRAVERSAL_MODE") };
}

/// RAII guard that sets `OMNIGRAPH_TRAVERSAL_MODE` and clears it on drop, so a
/// panic mid-test (e.g. a query `unwrap`) cannot leak the forced mode into a
/// later test in this binary. SAFE: every test here is `#[serial]` and this
/// binary has no non-serial env reader, so no thread reads the env during the
/// write. (Mirrors `proptest_equivalence.rs::ModeGuard`.)
struct ModeGuard;
impl ModeGuard {
    fn set(mode: &str) -> Self {
        set_mode(mode);
        ModeGuard
    }
}
impl Drop for ModeGuard {
    fn drop(&mut self) {
        clear_mode();
    }
}

/// First result column, sorted — for the probe-based topology-build tests below.
fn column0(result: &QueryResult) -> Vec<String> {
    if result.num_rows() == 0 {
        return Vec::new();
    }
    let batch = result.concat_batches().unwrap();
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut v: Vec<String> = (0..col.len()).map(|i| col.value(i).to_string()).collect();
    v.sort();
    v
}

/// Run a name-returning query and return its first column, sorted.
async fn sorted_names(db: &mut Omnigraph, queries: &str, name: &str, params: &ParamMap) -> Vec<String> {
    let result = query_main(db, queries, name, params).await.unwrap();
    if result.num_rows() == 0 {
        return Vec::new();
    }
    let batch = result.concat_batches().unwrap();
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut v: Vec<String> = (0..col.len()).map(|i| col.value(i).to_string()).collect();
    v.sort();
    v
}

/// Run the same query under CSR, indexed, and auto (cost-chooser) modes; assert
/// all three produce identical results and return them. The auto pass exercises
/// `choose_expand_mode` end to end: whichever path it selects, the rows must
/// match the forced paths (the chooser changes which path runs, never the result).
async fn both_modes(db: &mut Omnigraph, queries: &str, name: &str, params: &ParamMap) -> Vec<String> {
    set_mode("csr");
    let csr = sorted_names(db, queries, name, params).await;
    set_mode("indexed");
    let indexed = sorted_names(db, queries, name, params).await;
    clear_mode();
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
// Not #[serial] — it calls the helper directly and reads no env.
#[tokio::test]
async fn key_column_index_coverage_detects_btree_presence() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let snap = snapshot_main(&db).await.unwrap();

    // Edge `src` gets a BTREE from ensure_indices on load → Indexed.
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

    // Fresh load: the Knows BTREE covers every fragment → Indexed.
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
#[serial]
async fn indexed_matches_csr_one_hop_same_type() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    // friends_of: `$p knows $f` (Person -> Person, single hop).
    let got = both_modes(&mut db, TEST_QUERIES, "friends_of", &params(&[("$name", "Alice")])).await;
    assert_eq!(got, vec!["Bob", "Charlie"], "Alice knows Bob and Charlie");
}

#[tokio::test]
#[serial]
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
#[serial]
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
#[serial]
async fn indexed_matches_csr_no_match() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    // Diana has no outgoing Knows edges → empty in both modes.
    let got = both_modes(&mut db, TEST_QUERIES, "friends_of", &params(&[("$name", "Diana")])).await;
    assert!(got.is_empty(), "Diana knows no one");
}

#[tokio::test]
#[serial]
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

    set_mode("indexed");
    let got = sorted_names(&mut db, TEST_QUERIES, "friends_of", &params(&[("$name", "Alice")])).await;
    clear_mode();

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
#[serial]
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
#[serial]
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
#[serial]
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

// ─── Topology-index build cost (A1 cross-branch reuse + A2 scoped build) ─────
//
// These force the CSR build path (the indexed path builds no topology) and read
// the `graph_build_count` / `graph_edges_built` probes. They live HERE — the
// all-serial binary with no non-serial env reader — because they mutate the
// process-global `OMNIGRAPH_TRAVERSAL_MODE`, which `query.rs` reads; in a mixed
// serial/non-serial binary a concurrent non-serial traversal would race the env
// write. The `ModeGuard` clears the override even on a panic.

/// A1: a fresh (unwritten) branch reuses main's cached CSR topology index
/// (`graph_build_count == 0`), and the reused index returns correct results for
/// the branch. Before A1 the branch-keyed snapshot id forced a rebuild (count 1).
#[tokio::test]
#[serial]
async fn fresh_branch_traversal_reuses_main_graph_index() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = init_and_load(&dir).await;
    let uri = dir.path().to_str().unwrap();
    // A Knows edge on main so there is topology to build and then reuse.
    mutate_main(
        &mut writer,
        MUTATION_QUERIES,
        "insert_person_and_friend",
        &mixed_params(&[("$name", "Walker"), ("$friend", "Alice")], &[("$age", 41)]),
    )
    .await
    .unwrap();

    // Separate reader handle. As in production, the reader never creates the
    // branch, so creating it does not invalidate the reader's warm cache.
    let reader = Omnigraph::open(uri).await.unwrap();

    let _mode = ModeGuard::set("csr");
    // Reader warms main on the CSR path: builds and caches the topology index.
    let warm = reader
        .query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "friends_of",
            &params(&[("$name", "Walker")]),
        )
        .await
        .unwrap();
    assert_eq!(column0(&warm), vec!["Alice"], "test setup: main has the Knows edge");

    // A separate writer creates the branch (lazy fork: feature's edge tables are
    // physically main's — same version + e_tag, table_branch=None).
    writer.branch_create("feature").await.unwrap();

    let graph_build = Arc::new(AtomicU64::new(0));
    let probes_in = QueryIoProbes {
        graph_build_count: Arc::clone(&graph_build),
        ..Default::default()
    };
    let on_branch = with_query_io_probes(
        probes_in,
        reader.query(
            ReadTarget::branch("feature"),
            TEST_QUERIES,
            "friends_of",
            &params(&[("$name", "Walker")]),
        ),
    )
    .await
    .unwrap();

    assert_eq!(
        column0(&on_branch),
        vec!["Alice"],
        "fresh branch sees main's edges (lazy fork) and the reused index is correct"
    );
    assert_eq!(
        graph_build.load(Ordering::Relaxed),
        0,
        "a fresh branch with unchanged edges must reuse main's cached CSR index, not rebuild it"
    );
}

/// A2: a query referencing one edge type builds the topology for only that edge,
/// not every edge in the catalog. Forces CSR (the build path) and counts edge
/// tables built. Before A2 the build materialized all catalog edges (the fixture
/// defines Knows + WorksAt, so a build-all touches >= 2) — the cold-build cost.
#[tokio::test]
#[serial]
async fn single_edge_query_builds_only_referenced_edge() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    // A Knows edge so the referenced build has topology; the fixture also defines
    // WorksAt, so a build-all would touch more than one edge.
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person_and_friend",
        &mixed_params(&[("$name", "Walker"), ("$friend", "Alice")], &[("$age", 41)]),
    )
    .await
    .unwrap();

    let _mode = ModeGuard::set("csr");
    let graph_edges = Arc::new(AtomicU64::new(0));
    let probes_in = QueryIoProbes {
        graph_edges_built: Arc::clone(&graph_edges),
        ..Default::default()
    };
    let result = with_query_io_probes(
        probes_in,
        db.query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "friends_of",
            &params(&[("$name", "Walker")]),
        ),
    )
    .await
    .unwrap();

    assert_eq!(column0(&result), vec!["Alice"]);
    assert_eq!(
        graph_edges.load(Ordering::Relaxed),
        1,
        "a query referencing only `knows` must build only that edge, not all catalog edges"
    );
}
