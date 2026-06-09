//! Execution goldens for filtering by non-string/non-integer scalar LITERALS
//! (F64, F32, Bool, Date, DateTime), across both the in-memory comparison arm
//! (standalone `$m.prop op lit`) and the Lance-pushdown arm (inline binding
//! `Metric { prop: lit }`). Param-bound scalar filters and list-column
//! `contains` are already covered elsewhere; this closes the literal-RHS gap.

mod helpers;

use arrow_array::{Array, StringArray};

use omnigraph::db::Omnigraph;
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_compiler::ir::ParamMap;

use helpers::*;

const SCHEMA: &str = r#"
node Metric {
    name: String @key
    score: F64?
    ratio: F32?
    active: Bool?
    born: Date?
    seen: DateTime?
}
"#;

// Seeds partition every predicate, so a dropped filter returns all 4 rows.
const DATA: &str = r#"{"type":"Metric","data":{"name":"m1","score":2.5,"ratio":0.5,"active":true,"born":"2024-06-01","seen":"2024-06-01T12:00:00Z"}}
{"type":"Metric","data":{"name":"m2","score":1.0,"ratio":0.25,"active":false,"born":"2023-01-01","seen":"2023-01-01T00:00:00Z"}}
{"type":"Metric","data":{"name":"m3","score":3.0,"ratio":0.75,"active":true,"born":"2025-01-01","seen":"2025-01-01T00:00:00Z"}}
{"type":"Metric","data":{"name":"m4","score":0.5,"ratio":0.1,"active":false,"born":"2022-12-31","seen":"2022-01-01T00:00:00Z"}}"#;

async fn metric_db(dir: &tempfile::TempDir) -> Omnigraph {
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, SCHEMA).await.unwrap();
    load_jsonl(&mut db, DATA, LoadMode::Overwrite).await.unwrap();
    db
}

async fn sorted_metric_names(db: &mut Omnigraph, queries: &str, name: &str) -> Vec<String> {
    let r = query_main(db, queries, name, &ParamMap::new()).await.unwrap();
    if r.num_rows() == 0 {
        return Vec::new();
    }
    let b = r.concat_batches().unwrap();
    let col = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    let mut v: Vec<String> = (0..col.len()).map(|i| col.value(i).to_string()).collect();
    v.sort();
    v
}

#[tokio::test]
async fn float_literal_filters_execute() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = metric_db(&dir).await;
    let q = r#"
query gt() { match { $m: Metric  $m.score > 1.5 } return { $m.name } }
query le() { match { $m: Metric  $m.ratio <= 0.25 } return { $m.name } }
query inline() { match { $m: Metric { score: 3.0 } } return { $m.name } }
"#;
    // F64 standalone: scores 2.5, 3.0 > 1.5
    assert_eq!(sorted_metric_names(&mut db, q, "gt").await, vec!["m1", "m3"]);
    // F32 standalone: ratios 0.25, 0.1 <= 0.25
    assert_eq!(sorted_metric_names(&mut db, q, "le").await, vec!["m2", "m4"]);
    // F64 inline-binding pushdown: score == 3.0
    assert_eq!(sorted_metric_names(&mut db, q, "inline").await, vec!["m3"]);
}

#[tokio::test]
async fn bool_literal_filters_execute() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = metric_db(&dir).await;
    let q = r#"
query standalone() { match { $m: Metric  $m.active = true } return { $m.name } }
query inline() { match { $m: Metric { active: true } } return { $m.name } }
query negated() { match { $m: Metric  $m.active != true } return { $m.name } }
"#;
    assert_eq!(sorted_metric_names(&mut db, q, "standalone").await, vec!["m1", "m3"]);
    assert_eq!(sorted_metric_names(&mut db, q, "inline").await, vec!["m1", "m3"]);
    assert_eq!(sorted_metric_names(&mut db, q, "negated").await, vec!["m2", "m4"]);
}

#[tokio::test]
async fn date_and_datetime_literal_filters_execute() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = metric_db(&dir).await;
    let q = r#"
query born_ge() { match { $m: Metric  $m.born >= date("2024-01-01") } return { $m.name } }
query seen_lt() { match { $m: Metric  $m.seen < datetime("2024-01-01T00:00:00Z") } return { $m.name } }
"#;
    // born: m1 2024-06, m3 2025 >= 2024-01-01
    assert_eq!(sorted_metric_names(&mut db, q, "born_ge").await, vec!["m1", "m3"]);
    // seen: m2 2023, m4 2022 < 2024-01-01
    assert_eq!(sorted_metric_names(&mut db, q, "seen_lt").await, vec!["m2", "m4"]);
}
