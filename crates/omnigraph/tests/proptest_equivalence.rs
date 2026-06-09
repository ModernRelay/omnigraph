//! Property-based query-correctness invariants over generated graphs.
//!
//! The cross-type id-collision bug (fixed in f6a0e53) was a silent wrong-result
//! divergence between the two Expand modes, caught only because someone
//! hand-built the one colliding fixture. This turns that single example into a
//! search over the whole class: node keys for BOTH types are drawn from a small
//! SHARED alphabet, so cross-type collisions — plus cycles and self-loops —
//! arise frequently. The invariants make any future fork divergence (the planned
//! third ExpandMode, the anti-join fast/slow fork) fail loudly instead of
//! silently.
//!
//! Each test is a sync `#[test]` + `#[serial]`: it builds its own runtime and
//! `block_on`s per generated case (proptest closures are sync), and the
//! mode-equivalence test writes `OMNIGRAPH_TRAVERSAL_MODE`, so serial execution
//! keeps env writes from racing other tests in this binary.

mod helpers;

use std::collections::HashSet;

use arrow_array::{Array, StringArray};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use serial_test::serial;

use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_compiler::ir::ParamMap;
use omnigraph_compiler::query::ast::Literal;

use helpers::*;

/// Small SHARED key alphabet — Person and Company keys are both drawn from this,
/// so cross-type id collisions are common.
const KEYS: &[&str] = &["a", "b", "c", "d", "e"];

const QUERIES: &str = r#"
query friends($name: String) {
    match {
        $p: Person { name: $name }
        $p knows{1,3} $f
    }
    return { $f.name }
}
query employers($name: String) {
    match {
        $p: Person { name: $name }
        $p worksAt{1,2} $c
    }
    return { $c.name }
}
query all_persons() {
    match { $p: Person }
    return { $p.name }
}
query employed() {
    match {
        $p: Person
        $p worksAt $c
    }
    return { $p.name }
}
query unemployed() {
    match {
        $p: Person
        not { $p worksAt $_ }
    }
    return { $p.name }
}
"#;

#[derive(Debug, Clone)]
struct GenGraph {
    persons: Vec<String>,
    companies: Vec<String>,
    knows: Vec<(usize, usize)>,    // indices into persons (self-loops & cycles allowed)
    works_at: Vec<(usize, usize)>, // (person idx, company idx)
}

impl GenGraph {
    fn to_jsonl(&self) -> String {
        let mut s = String::new();
        for p in &self.persons {
            s.push_str(&format!("{{\"type\":\"Person\",\"data\":{{\"name\":\"{p}\"}}}}\n"));
        }
        for c in &self.companies {
            s.push_str(&format!("{{\"type\":\"Company\",\"data\":{{\"name\":\"{c}\"}}}}\n"));
        }
        // Dedup exact-duplicate edge rows (the loader rejects intra-batch
        // duplicate keys); collisions/cycles/self-loops are unaffected.
        let mut seen = HashSet::new();
        for &(a, b) in &self.knows {
            if seen.insert(("k", a, b)) {
                s.push_str(&format!(
                    "{{\"edge\":\"Knows\",\"from\":\"{}\",\"to\":\"{}\"}}\n",
                    self.persons[a], self.persons[b]
                ));
            }
        }
        for &(a, b) in &self.works_at {
            if seen.insert(("w", a, b)) {
                s.push_str(&format!(
                    "{{\"edge\":\"WorksAt\",\"from\":\"{}\",\"to\":\"{}\"}}\n",
                    self.persons[a], self.companies[b]
                ));
            }
        }
        s
    }
}

fn arb_keys() -> impl Strategy<Value = Vec<String>> {
    proptest::sample::subsequence(KEYS.to_vec(), 1..=KEYS.len())
        .prop_map(|v| v.into_iter().map(String::from).collect())
}

fn arb_graph() -> impl Strategy<Value = GenGraph> {
    (arb_keys(), arb_keys()).prop_flat_map(|(persons, companies)| {
        let np = persons.len();
        let nc = companies.len();
        let knows = prop::collection::vec((0..np, 0..np), 0..=10);
        let works = prop::collection::vec((0..np, 0..nc), 0..=10);
        (Just(persons), Just(companies), knows, works).prop_map(
            |(persons, companies, knows, works_at)| GenGraph {
                persons,
                companies,
                knows,
                works_at,
            },
        )
    })
}

fn config() -> Config {
    Config {
        cases: 48,
        ..Config::default()
    }
}

fn clear_mode() {
    unsafe { std::env::remove_var("OMNIGRAPH_TRAVERSAL_MODE") };
}

/// RAII guard that sets `OMNIGRAPH_TRAVERSAL_MODE` and clears it on drop — so a
/// panic mid-case (e.g. a query `unwrap`) cannot leak the forced mode into
/// proptest's subsequent shrink/cases and mask the divergence under test. SAFE:
/// every test in this binary is `#[serial]`, so no thread reads the env during
/// the write.
struct ModeGuard;
impl ModeGuard {
    fn set(mode: &str) -> Self {
        unsafe { std::env::set_var("OMNIGRAPH_TRAVERSAL_MODE", mode) };
        ModeGuard
    }
}
impl Drop for ModeGuard {
    fn drop(&mut self) {
        unsafe { std::env::remove_var("OMNIGRAPH_TRAVERSAL_MODE") };
    }
}

async fn load_graph(graph: &GenGraph) -> (tempfile::TempDir, Omnigraph) {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&mut db, &graph.to_jsonl(), LoadMode::Overwrite)
        .await
        .unwrap();
    (dir, db)
}

fn one_param(val: &str) -> ParamMap {
    let mut m = ParamMap::new();
    m.insert("name".to_string(), Literal::String(val.to_string()));
    m
}

/// First-column strings, sorted (MULTISET — preserves duplicate-row count so
/// mode comparisons catch dedup divergence, not just set divergence).
async fn col0_sorted(db: &mut Omnigraph, name: &str, params: &ParamMap) -> Vec<String> {
    let r = db
        .query(ReadTarget::branch("main"), QUERIES, name, params)
        .await
        .unwrap();
    if r.num_rows() == 0 {
        return Vec::new();
    }
    let b = r.concat_batches().unwrap();
    let col = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    let mut v: Vec<String> = (0..col.len()).map(|i| col.value(i).to_string()).collect();
    v.sort();
    v
}

async fn col0_set(db: &mut Omnigraph, name: &str, params: &ParamMap) -> HashSet<String> {
    col0_sorted(db, name, params).await.into_iter().collect()
}

// INVARIANT 1: mode equivalence. For any generated graph and start key, the
// CSR, indexed, and auto paths return identical result multisets — over both a
// same-type traversal (knows{1,3}, exercises cycles/self-loops) and a cross-type
// one (worksAt{1,2}, collision-prone). This is the search-over-the-class version
// of the hand-built cross-type-collision fixture.
#[test]
#[serial]
fn prop_expand_indexed_eq_csr() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut runner = TestRunner::new(config());
    runner
        .run(&arb_graph(), |graph| {
            let mismatch = rt.block_on(async {
                let (_dir, mut db) = load_graph(&graph).await;
                for start in graph.persons.clone() {
                    let p = one_param(&start);
                    for q in ["friends", "employers"] {
                        // Each guard clears the mode on drop (end of the block,
                        // or on panic), so a forced mode never leaks across runs.
                        let csr = {
                            let _g = ModeGuard::set("csr");
                            col0_sorted(&mut db, q, &p).await
                        };
                        let indexed = {
                            let _g = ModeGuard::set("indexed");
                            col0_sorted(&mut db, q, &p).await
                        };
                        // No guard → env unset → auto (cost-based) path.
                        let auto = col0_sorted(&mut db, q, &p).await;
                        if csr != indexed || csr != auto {
                            return Some((start, q, csr, indexed, auto));
                        }
                    }
                }
                None
            });
            prop_assert!(
                mismatch.is_none(),
                "Expand mode divergence: {:?}",
                mismatch
            );
            Ok(())
        })
        .unwrap();
}

// INVARIANT 2: no phantom rows. Every key a traversal returns must belong to the
// destination type's loaded key set — independent of the two-mode comparison, so
// it catches over-emission even if both modes are wrong identically.
#[test]
#[serial]
fn prop_results_subset_of_existing_nodes() {
    clear_mode();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut runner = TestRunner::new(config());
    runner
        .run(&arb_graph(), |graph| {
            let bad = rt.block_on(async {
                let (_dir, mut db) = load_graph(&graph).await;
                let persons: HashSet<String> = graph.persons.iter().cloned().collect();
                let companies: HashSet<String> = graph.companies.iter().cloned().collect();
                for start in graph.persons.clone() {
                    let p = one_param(&start);
                    for f in col0_set(&mut db, "friends", &p).await {
                        if !persons.contains(&f) {
                            return Some(("friends", start, f));
                        }
                    }
                    for c in col0_set(&mut db, "employers", &p).await {
                        if !companies.contains(&c) {
                            return Some(("employers", start, c));
                        }
                    }
                }
                None
            });
            prop_assert!(bad.is_none(), "phantom row: {:?}", bad);
            Ok(())
        })
        .unwrap();
}

// INVARIANT 3: anti-join complement. `not { $p worksAt $_ }` and its complement
// (persons WITH a worksAt) must be disjoint and together cover all persons.
#[test]
#[serial]
fn prop_antijoin_partitions_persons() {
    clear_mode();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut runner = TestRunner::new(config());
    runner
        .run(&arb_graph(), |graph| {
            let err = rt.block_on(async {
                let (_dir, mut db) = load_graph(&graph).await;
                let all = col0_set(&mut db, "all_persons", &ParamMap::new()).await;
                let unemployed = col0_set(&mut db, "unemployed", &ParamMap::new()).await;
                let employed = col0_set(&mut db, "employed", &ParamMap::new()).await;
                let overlap: Vec<_> = unemployed.intersection(&employed).cloned().collect();
                let union: HashSet<_> = unemployed.union(&employed).cloned().collect();
                if !overlap.is_empty() {
                    return Some(format!("overlap {overlap:?}"));
                }
                if union != all {
                    return Some(format!("union {union:?} != all {all:?}"));
                }
                None
            });
            prop_assert!(err.is_none(), "anti-join partition broken: {:?}", err);
            Ok(())
        })
        .unwrap();
}
