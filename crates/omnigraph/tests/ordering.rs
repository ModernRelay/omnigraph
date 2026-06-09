//! ORDER BY golden coverage: descending, multi-key precedence, deterministic
//! tie-break (total order), and NULL placement.
//!
//! These pin the observable output-ordering contract (deny-list: "output
//! ordering … become dependencies once shipped"). `apply_ordering` appends the
//! bound entities' key columns as an ascending tie-break, so equal user-sort
//! keys yield a TOTAL, deterministic order (and `ORDER … LIMIT` is
//! deterministic). NULL placement is `nulls_first = !descending` (NULLs first
//! under ASC, last under DESC). Both are documented in
//! `docs/user/query-language.md`.

mod helpers;

use arrow_array::{Array, StringArray};

use omnigraph::db::Omnigraph;
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_compiler::ir::ParamMap;
use omnigraph_compiler::result::QueryResult;

use helpers::*;

/// Names in result ROW order (not sorted) — these tests assert positional order.
fn names_in_order(result: &QueryResult) -> Vec<String> {
    let batch = result.concat_batches().unwrap();
    if batch.num_rows() == 0 {
        return Vec::new();
    }
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..col.len()).map(|i| col.value(i).to_string()).collect()
}

/// Init the standard schema and load a custom Person-only dataset.
async fn init_people(dir: &tempfile::TempDir, jsonl: &str) -> Omnigraph {
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&mut db, jsonl, LoadMode::Overwrite).await.unwrap();
    db
}

#[tokio::test]
async fn ordering_descending() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let q = r#"
query q() {
    match { $p: Person }
    return { $p.name }
    order { $p.age desc }
}
"#;
    let got = names_in_order(&query_main(&mut db, q, "q", &ParamMap::new()).await.unwrap());
    // Charlie(35), Alice(30), Diana(28), Bob(25)
    assert_eq!(got, vec!["Charlie", "Alice", "Diana", "Bob"]);
}

#[tokio::test]
async fn ordering_multi_key_age_desc_name_asc() {
    let dir = tempfile::tempdir().unwrap();
    // Alice & Bob tie at age 30; loaded Bob-first so the expected output order
    // cannot be the load order.
    let data = r#"{"type":"Person","data":{"name":"Bob","age":30}}
{"type":"Person","data":{"name":"Alice","age":30}}
{"type":"Person","data":{"name":"Charlie","age":25}}"#;
    let mut db = init_people(&dir, data).await;
    let q = r#"
query q() {
    match { $p: Person }
    return { $p.name }
    order { $p.age desc, $p.name asc }
}
"#;
    let got = names_in_order(&query_main(&mut db, q, "q", &ParamMap::new()).await.unwrap());
    // age desc -> [30,30,25]; the 30-tie broken by name asc -> Alice before Bob.
    assert_eq!(got, vec!["Alice", "Bob", "Charlie"]);
}

#[tokio::test]
async fn ordering_tiebreak_by_key_is_deterministic() {
    let dir = tempfile::tempdir().unwrap();
    // Same tie at age 30, NO secondary sort key. Loaded Bob-first; the tie must
    // break by the entity key (name) ascending -> Alice before Bob, regardless
    // of load order. This locks the total-order tie-break in apply_ordering.
    let data = r#"{"type":"Person","data":{"name":"Bob","age":30}}
{"type":"Person","data":{"name":"Alice","age":30}}
{"type":"Person","data":{"name":"Charlie","age":25}}"#;
    let mut db = init_people(&dir, data).await;
    let q = r#"
query q() {
    match { $p: Person }
    return { $p.name }
    order { $p.age asc }
}
"#;
    let got = names_in_order(&query_main(&mut db, q, "q", &ParamMap::new()).await.unwrap());
    // age asc -> Charlie(25), then the 30-tie broken by key asc -> Alice, Bob.
    assert_eq!(got, vec!["Charlie", "Alice", "Bob"]);
}

#[tokio::test]
async fn ordering_nulls_placement_asc_and_desc() {
    let dir = tempfile::tempdir().unwrap();
    // Bob has a NULL age.
    let data = r#"{"type":"Person","data":{"name":"Alice","age":30}}
{"type":"Person","data":{"name":"Bob","age":null}}
{"type":"Person","data":{"name":"Charlie","age":25}}"#;
    let mut db = init_people(&dir, data).await;

    let asc = r#"
query q() {
    match { $p: Person }
    return { $p.name }
    order { $p.age asc }
}
"#;
    let got_asc = names_in_order(&query_main(&mut db, asc, "q", &ParamMap::new()).await.unwrap());
    // ASC: nulls_first -> Bob(null), then 25, 30.
    assert_eq!(got_asc, vec!["Bob", "Charlie", "Alice"]);

    let desc = r#"
query q() {
    match { $p: Person }
    return { $p.name }
    order { $p.age desc }
}
"#;
    let got_desc = names_in_order(&query_main(&mut db, desc, "q", &ParamMap::new()).await.unwrap());
    // DESC: nulls last -> 30, 25, then Bob(null).
    assert_eq!(got_desc, vec!["Alice", "Charlie", "Bob"]);
}
