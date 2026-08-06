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
    count: I32?
    active: Bool?
    born: Date?
    seen: DateTime?
}
"#;

// Seeds partition every predicate, so a dropped filter returns all 4 rows.
const DATA: &str = r#"{"type":"Metric","data":{"name":"m1","score":2.5,"ratio":0.5,"count":1,"active":true,"born":"2024-06-01","seen":"2024-06-01T12:00:00Z"}}
{"type":"Metric","data":{"name":"m2","score":1.0,"ratio":0.25,"count":2,"active":false,"born":"2023-01-01","seen":"2023-01-01T00:00:00Z"}}
{"type":"Metric","data":{"name":"m3","score":3.0,"ratio":0.75,"count":3,"active":true,"born":"2025-01-01","seen":"2025-01-01T00:00:00Z"}}
{"type":"Metric","data":{"name":"m4","score":0.5,"ratio":0.1,"count":4,"active":false,"born":"2022-12-31","seen":"2022-01-01T00:00:00Z"}}"#;

async fn metric_db(dir: &tempfile::TempDir) -> Omnigraph {
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, SCHEMA).await.unwrap();
    load_jsonl(&mut db, DATA, LoadMode::Overwrite)
        .await
        .unwrap();
    db
}

async fn sorted_metric_names(db: &mut Omnigraph, queries: &str, name: &str) -> Vec<String> {
    let r = query_main(db, queries, name, &ParamMap::new())
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
    assert_eq!(
        sorted_metric_names(&mut db, q, "gt").await,
        vec!["m1", "m3"]
    );
    // F32 standalone: ratios 0.25, 0.1 <= 0.25
    assert_eq!(
        sorted_metric_names(&mut db, q, "le").await,
        vec!["m2", "m4"]
    );
    // F64 inline-binding pushdown: score == 3.0
    assert_eq!(sorted_metric_names(&mut db, q, "inline").await, vec!["m3"]);
}

// Inline-binding equality is the Lance-pushdown arm. With the literal coerced to
// the column's exact Arrow type, a narrow-numeric column (I32) and an F32 column
// must still select the right rows — the coercion changes the literal's type, not
// the result set. (The index-use win this enables is pinned at the Lance-surface
// layer by `lance_surface_guards::scalar_index_use_requires_matched_literal_type`.)
#[tokio::test]
async fn int_and_f32_literal_pushdown_coercion() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = metric_db(&dir).await;
    let q = r#"
query count_eq() { match { $m: Metric { count: 2 } } return { $m.name } }
query ratio_eq() { match { $m: Metric { ratio: 0.25 } } return { $m.name } }
query count_ge() { match { $m: Metric  $m.count >= 3 } return { $m.name } }
"#;
    // I32 column, integer literal coerced Int64 -> Int32: count == 2 is m2 only.
    assert_eq!(
        sorted_metric_names(&mut db, q, "count_eq").await,
        vec!["m2"]
    );
    // F32 column, float literal coerced Float64 -> Float32: ratio == 0.25 is m2.
    assert_eq!(
        sorted_metric_names(&mut db, q, "ratio_eq").await,
        vec!["m2"]
    );
    // Range on the I32 column: count 3,4 >= 3 -> m3, m4 (coercion is op-independent).
    assert_eq!(
        sorted_metric_names(&mut db, q, "count_ge").await,
        vec!["m3", "m4"]
    );
}

// A fractional float against an integer column must not be truncated by the
// pushdown coercion (`2.7 -> 2` would wrongly match the count=2 row). The
// lossless guard falls back to the natural Float64 literal, so `count = 2.7`
// matches no integer and returns no rows.
#[tokio::test]
async fn fractional_float_equality_on_int_column_returns_no_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = metric_db(&dir).await;
    let q = r#"
query count_frac() { match { $m: Metric { count: 2.7 } } return { $m.name } }
"#;
    assert!(
        sorted_metric_names(&mut db, q, "count_frac")
            .await
            .is_empty(),
        "count = 2.7 must match no integer rows (no truncation to count = 2)"
    );
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
    assert_eq!(
        sorted_metric_names(&mut db, q, "standalone").await,
        vec!["m1", "m3"]
    );
    assert_eq!(
        sorted_metric_names(&mut db, q, "inline").await,
        vec!["m1", "m3"]
    );
    assert_eq!(
        sorted_metric_names(&mut db, q, "negated").await,
        vec!["m2", "m4"]
    );
}

#[tokio::test]
async fn date_and_datetime_literal_filters_execute() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = metric_db(&dir).await;
    let q = r#"
query born_ge() { match { $m: Metric  $m.born >= date("2024-01-01") } return { $m.name } }
query seen_lt() { match { $m: Metric  $m.seen < datetime("2024-01-01T00:00:00Z") } return { $m.name } }
query born_eq() { match { $m: Metric { born: date("2024-06-01") } } return { $m.name } }
query seen_eq() { match { $m: Metric { seen: datetime("2024-06-01T12:00:00Z") } } return { $m.name } }
"#;
    // born: m1 2024-06, m3 2025 >= 2024-01-01
    assert_eq!(
        sorted_metric_names(&mut db, q, "born_ge").await,
        vec!["m1", "m3"]
    );
    // seen: m2 2023, m4 2022 < 2024-01-01
    assert_eq!(
        sorted_metric_names(&mut db, q, "seen_lt").await,
        vec!["m2", "m4"]
    );
    // Inline-binding equality exercises the Lance-pushdown arm with a typed
    // Date32/Date64 literal: the epoch conversion must select exactly m1.
    assert_eq!(sorted_metric_names(&mut db, q, "born_eq").await, vec!["m1"]);
    assert_eq!(sorted_metric_names(&mut db, q, "seen_eq").await, vec!["m1"]);
}

// #283: a property-match on a camelCase `@index` field must execute, not fail
// with "No field named reponame" at the Lance scan. Exercises the pushdown arm
// (inline binding `Doc { repoName: $r }`) end-to-end.
const CC_SCHEMA: &str = r#"
node Doc {
    slug: String @key
    repoName: String @index
}
"#;
const CC_DATA: &str = r#"{"type":"Doc","data":{"slug":"d1","repoName":"acme"}}
{"type":"Doc","data":{"slug":"d2","repoName":"globex"}}"#;

#[tokio::test]
async fn camelcase_property_filter_executes() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, CC_SCHEMA).await.unwrap();
    load_jsonl(&mut db, CC_DATA, LoadMode::Overwrite)
        .await
        .unwrap();

    let q =
        r#"query by_repo($r: String) { match { $d: Doc { repoName: $r } } return { $d.slug } }"#;
    let r = query_main(&mut db, q, "by_repo", &params(&[("$r", "acme")]))
        .await
        .expect("camelCase property filter must execute, not fail at the Lance scan");
    assert_eq!(
        r.num_rows(),
        1,
        "expected exactly the d1 row for repoName=acme"
    );
}
