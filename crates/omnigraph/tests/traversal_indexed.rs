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

use arrow_array::{Array, StringArray};

use omnigraph::db::Omnigraph;
use omnigraph::table_store::{IndexCoverage, TableStore};
use omnigraph_compiler::ir::ParamMap;
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

/// Run the same query under CSR then indexed mode; assert identical results and
/// return them.
async fn both_modes(db: &mut Omnigraph, queries: &str, name: &str, params: &ParamMap) -> Vec<String> {
    set_mode("csr");
    let csr = sorted_names(db, queries, name, params).await;
    set_mode("indexed");
    let indexed = sorted_names(db, queries, name, params).await;
    clear_mode();
    assert_eq!(
        indexed, csr,
        "indexed Expand must produce identical results to CSR for query '{name}'"
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
